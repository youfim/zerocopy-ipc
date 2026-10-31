from typing import Optional, Union, Any
try:
    from ._core import Producer as _CoreProducer, Consumer as _CoreConsumer, FrameLease, DataType, PeerDiedError
except ImportError:
    # Fallback when running against directly compiled .pyd in package directory
    import sys
    from pathlib import Path
    pkg_dir = Path(__file__).resolve().parent
    if str(pkg_dir) not in sys.path:
        sys.path.insert(0, str(pkg_dir))
    from _core import Producer as _CoreProducer, Consumer as _CoreConsumer, FrameLease, DataType, PeerDiedError

def _frame_lease_to_torch(self):
    """
    Zero-copy export directly into a PyTorch Tensor sharing the underlying shared memory.
    """
    import torch
    arr = self.to_numpy()
    return torch.from_numpy(arr)

FrameLease.to_torch = _frame_lease_to_torch

class ProducerChannel:
    """
    High-level zero-copy shared memory IPC Producer.
    """
    def __init__(self, name: str, ring_capacity: int = 128, data_arena_capacity: int = 64 * 1024 * 1024):
        self._core = _CoreProducer(name, ring_capacity, data_arena_capacity)
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    @property
    def capacity(self) -> int:
        return self._core.capacity()

    @property
    def arena_capacity(self) -> int:
        return self._core.arena_capacity()

    def send_bytes(self, data: bytes, spin_count: int = 2048, timeout_ms: int = 1000) -> bool:
        return self._core.send_bytes(data, spin_count, timeout_ms)

    def send_tensor(self, tensor: Any, spin_count: int = 2048, timeout_ms: int = 1000) -> bool:
        """
        Sends a NumPy ndarray or PyTorch CPU tensor into shared memory with zero serialization overhead.
        """
        # PyTorch support: convert CPU tensor to contiguous NumPy array without copy
        if hasattr(tensor, "detach") and hasattr(tensor, "cpu") and hasattr(tensor, "numpy"):
            tensor = tensor.detach().cpu().numpy()

        import numpy as np
        if not isinstance(tensor, np.ndarray):
            raise TypeError(f"Expected numpy.ndarray or torch.Tensor, got {type(tensor)}")

        if not tensor.flags["C_CONTIGUOUS"]:
            tensor = np.ascontiguousarray(tensor)

        return self._core.send_array(tensor, spin_count, timeout_ms)

    def close(self) -> None:
        self._core.shutdown()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class ConsumerChannel:
    """
    High-level zero-copy shared memory IPC Consumer.
    """
    def __init__(self, name: str, wait_ready_ms: int = 5000):
        self._core = _CoreConsumer(name, wait_ready_ms)
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    @property
    def capacity(self) -> int:
        return self._core.capacity()

    @property
    def arena_capacity(self) -> int:
        return self._core.arena_capacity()

    def receive(self, spin_count: int = 2048, timeout_ms: int = 1000) -> Optional[FrameLease]:
        """
        Acquires next available frame as a context manager FrameLease.
        Usage:
            with consumer.receive() as frame:
                arr = frame.to_numpy()
                # process arr
            # slot tail automatically advances upon exit
        """
        return self._core.receive(spin_count, timeout_ms)

    def close(self) -> None:
        self._core.shutdown()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

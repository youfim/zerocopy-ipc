import unittest
import numpy as np
import time
import multiprocessing as mp
from zero_copy_ipc import ProducerChannel, ConsumerChannel, DataType

class TestZeroCopyIpc(unittest.TestCase):
    def test_bytes_ping_pong(self):
        name = "test_py_bytes"
        with ProducerChannel(name, ring_capacity=32, data_arena_capacity=1024*1024) as prod:
            with ConsumerChannel(name) as cons:
                data = b"Hello zero-copy IPC from Python!"
                ok = prod.send_bytes(data)
                self.assertTrue(ok)

                with cons.receive(timeout_ms=1000) as frame:
                    self.assertIsNotNone(frame)
                    self.assertTrue(frame.is_valid())
                    self.assertEqual(frame.size(), len(data))
                    self.assertEqual(frame.to_bytes(), data)
                    mv = frame.to_memoryview()
                    self.assertEqual(bytes(mv), data)

                self.assertFalse(frame.is_valid())

    def test_numpy_zero_copy_ndarray(self):
        name = "test_py_numpy_zc"
        with ProducerChannel(name, ring_capacity=32, data_arena_capacity=16*1024*1024) as prod:
            with ConsumerChannel(name) as cons:
                # 1080p RGB image simulation: (1080, 1920, 3) uint8
                shape = (1080, 1920, 3)
                arr = np.random.randint(0, 256, size=shape, dtype=np.uint8)

                ok = prod.send_tensor(arr)
                self.assertTrue(ok)

                with cons.receive(timeout_ms=1000) as frame:
                    self.assertIsNotNone(frame)
                    self.assertEqual(frame.ndim(), 3)
                    self.assertEqual(tuple(frame.shape()), shape)
                    
                    # Zero-copy view
                    view = frame.to_numpy()
                    self.assertEqual(view.shape, shape)
                    self.assertEqual(view.dtype, np.uint8)
                    np.testing.assert_array_equal(view, arr)

                    # Pointer verification: verify ndarray data buffer points directly to SHM
                    view_ptr = view.__array_interface__["data"][0]
                    self.assertEqual(view_ptr, frame.data_ptr())

                # Verifying lease expiration
                with self.assertRaises(RuntimeError):
                    _ = frame.to_numpy()

    def test_numpy_float32_float64_tensors(self):
        name = "test_py_dtypes"
        with ProducerChannel(name, ring_capacity=32, data_arena_capacity=4*1024*1024) as prod:
            with ConsumerChannel(name) as cons:
                # Float32 matrix
                mat_f32 = np.linspace(0.0, 1.0, 10000, dtype=np.float32).reshape(100, 100)
                prod.send_tensor(mat_f32)
                with cons.receive(timeout_ms=500) as frame:
                    rec_f32 = frame.to_numpy()
                    self.assertEqual(rec_f32.dtype, np.float32)
                    np.testing.assert_allclose(rec_f32, mat_f32)

                # Float64 vector
                vec_f64 = np.arange(500, dtype=np.float64)
                prod.send_tensor(vec_f64)
                with cons.receive(timeout_ms=500) as frame:
                    rec_f64 = frame.to_numpy()
                    self.assertEqual(rec_f64.dtype, np.float64)
                    np.testing.assert_allclose(rec_f64, vec_f64)

def _producer_proc(name, count):
    with ProducerChannel(name, ring_capacity=64, data_arena_capacity=8*1024*1024) as prod:
        for i in range(count):
            arr = np.full((128, 128), fill_value=i, dtype=np.int32)
            while not prod.send_tensor(arr, timeout_ms=50):
                pass

def _consumer_proc(name, count, return_queue):
    received = 0
    with ConsumerChannel(name) as cons:
        for i in range(count):
            with cons.receive(timeout_ms=2000) as frame:
                if frame is None:
                    break
                arr = frame.to_numpy()
                if arr[0, 0] != i:
                    return_queue.put(f"Corruption at index {i}: expected {i}, got {arr[0, 0]}")
                    return
                received += 1
    return_queue.put(received)

def _crash_producer_proc(name, ready_event):
    prod = ProducerChannel(name, ring_capacity=16, data_arena_capacity=1024*1024)
    prod.send_bytes(b"initial_frame")
    ready_event.set()
    time.sleep(60)

class TestCrossProcessIpc(unittest.TestCase):
    def test_multiprocess_tensor_stream(self):
        name = "test_py_mproc"
        count = 1000
        q = mp.Queue()

        p_prod = mp.Process(target=_producer_proc, args=(name, count))
        p_prod.start()
        time.sleep(0.05)

        p_cons = mp.Process(target=_consumer_proc, args=(name, count, q))
        p_cons.start()

        p_prod.join(timeout=10)
        p_cons.join(timeout=10)

        self.assertFalse(q.empty(), "Consumer did not return result")
        result = q.get()
        self.assertEqual(result, count, f"Failed multiprocess test with result: {result}")

    def test_peer_crash_detection(self):
        from zero_copy_ipc import PeerDiedError
        name = "test_py_crash"
        ready = mp.Event()

        p_prod = mp.Process(target=_crash_producer_proc, args=(name, ready))
        p_prod.start()

        ready.wait(timeout=5)
        with ConsumerChannel(name) as cons:
            with cons.receive(timeout_ms=1000) as frame:
                self.assertIsNotNone(frame)
                self.assertEqual(frame.to_bytes(), b"initial_frame")

            # Forcibly terminate producer process
            p_prod.terminate()
            p_prod.join(timeout=3)

            # Next receive must immediately raise PeerDiedError instead of waiting
            with self.assertRaises(PeerDiedError):
                _ = cons.receive(timeout_ms=1000)

if __name__ == "__main__":
    unittest.main()

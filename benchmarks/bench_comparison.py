import time
import multiprocessing as mp
import socket
import numpy as np
from zero_copy_ipc import ProducerChannel, ConsumerChannel

# 1080p RGB frame (~6.22 MB)
FRAME_SHAPE = (1080, 1920, 3)
FRAME_DTYPE = np.uint8
FRAME_BYTES = int(np.prod(FRAME_SHAPE) * np.dtype(FRAME_DTYPE).itemsize)
NUM_FRAMES = 100

def _zc_prod_worker(name, num_frames, ready_event):
    with ProducerChannel(name, ring_capacity=32, data_arena_capacity=128 * 1024 * 1024) as prod:
        ready_event.set()
        frame = np.ones(FRAME_SHAPE, dtype=FRAME_DTYPE)
        for _ in range(num_frames):
            while not prod.send_tensor(frame, timeout_ms=500):
                pass

def _zc_cons_worker(name, num_frames, ready_event, res_q):
    ready_event.wait()
    with ConsumerChannel(name) as cons:
        t0 = time.perf_counter()
        for _ in range(num_frames):
            with cons.receive(timeout_ms=1000) as f:
                _ = f.to_numpy()
        t1 = time.perf_counter()
        res_q.put(t1 - t0)

def bench_zerocopy_ipc():
    name = "bench_comp_shm"
    res_q = mp.Queue()
    ready_event = mp.Event()

    p1 = mp.Process(target=_zc_prod_worker, args=(name, NUM_FRAMES, ready_event))
    p2 = mp.Process(target=_zc_cons_worker, args=(name, NUM_FRAMES, ready_event, res_q))

    p1.start()
    p2.start()
    p1.join()
    p2.join()

    elapsed = res_q.get()
    mb_transferred = (FRAME_BYTES * NUM_FRAMES) / (1024 * 1024)
    fps = NUM_FRAMES / elapsed
    bandwidth = mb_transferred / elapsed
    return elapsed, fps, bandwidth

def _mp_prod_worker(q, num_frames):
    frame = np.ones(FRAME_SHAPE, dtype=FRAME_DTYPE)
    for _ in range(num_frames):
        q.put(frame)

def _mp_cons_worker(q, num_frames, res_q):
    t0 = time.perf_counter()
    for _ in range(num_frames):
        _ = q.get()
    t1 = time.perf_counter()
    res_q.put(t1 - t0)

def bench_multiprocessing_queue():
    q = mp.Queue(maxsize=16)
    res_q = mp.Queue()
    p1 = mp.Process(target=_mp_prod_worker, args=(q, NUM_FRAMES))
    p2 = mp.Process(target=_mp_cons_worker, args=(q, NUM_FRAMES, res_q))

    p1.start()
    p2.start()
    p1.join()
    p2.join()

    elapsed = res_q.get()
    mb_transferred = (FRAME_BYTES * NUM_FRAMES) / (1024 * 1024)
    fps = NUM_FRAMES / elapsed
    bandwidth = mb_transferred / elapsed
    return elapsed, fps, bandwidth

def _tcp_server_worker(port, num_frames, ready_event):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    srv.bind(("127.0.0.1", port))
    srv.listen(1)
    ready_event.set()
    conn, _ = srv.accept()
    frame = np.ones(FRAME_SHAPE, dtype=FRAME_DTYPE).tobytes()
    for _ in range(num_frames):
        conn.sendall(frame)
    conn.close()
    srv.close()

def _tcp_client_worker(port, num_frames, ready_event, res_q):
    ready_event.wait()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    sock.connect(("127.0.0.1", port))

    buf = bytearray(FRAME_BYTES)
    view = memoryview(buf)
    t0 = time.perf_counter()
    for _ in range(num_frames):
        remaining = FRAME_BYTES
        offset = 0
        while remaining > 0:
            n = sock.recv_into(view[offset:], remaining)
            if not n:
                break
            offset += n
            remaining -= n
    t1 = time.perf_counter()
    sock.close()
    res_q.put(t1 - t0)

def bench_tcp_socket():
    port = 54321
    res_q = mp.Queue()
    ready_event = mp.Event()
    p1 = mp.Process(target=_tcp_server_worker, args=(port, NUM_FRAMES, ready_event))
    p2 = mp.Process(target=_tcp_client_worker, args=(port, NUM_FRAMES, ready_event, res_q))

    p1.start()
    p2.start()
    p1.join()
    p2.join()

    elapsed = res_q.get()
    mb_transferred = (FRAME_BYTES * NUM_FRAMES) / (1024 * 1024)
    fps = NUM_FRAMES / elapsed
    bandwidth = mb_transferred / elapsed
    return elapsed, fps, bandwidth

def main():
    print("=" * 70)
    print("      IPC Benchmark Comparison: 1080p RGB Frames (6.22 MB)")
    print(f"      Total Transferred: {NUM_FRAMES} frames (~{(FRAME_BYTES * NUM_FRAMES) / (1024*1024):.1f} MB)")
    print("=" * 70)

    print("\nRunning multiprocessing.Queue benchmark...")
    mp_time, mp_fps, mp_bw = bench_multiprocessing_queue()
    print(f"multiprocessing.Queue     : {mp_time:.3f} s | {mp_fps:6.1f} FPS | {mp_bw:7.1f} MB/s")

    print("\nRunning TCP loopback socket benchmark...")
    tcp_time, tcp_fps, tcp_bw = bench_tcp_socket()
    print(f"TCP loopback socket       : {tcp_time:.3f} s | {tcp_fps:6.1f} FPS | {tcp_bw:7.1f} MB/s")

    print("\nRunning zero-copy shared memory IPC benchmark...")
    zc_time, zc_fps, zc_bw = bench_zerocopy_ipc()
    print(f"zero-copy IPC (nanobind)  : {zc_time:.3f} s | {zc_fps:6.1f} FPS | {zc_bw:7.1f} MB/s")

    speedup_mp = zc_bw / mp_bw if mp_bw > 0 else 0
    speedup_tcp = zc_bw / tcp_bw if tcp_bw > 0 else 0
    print("\n" + "=" * 70)
    print(f"Speedup vs multiprocessing.Queue : {speedup_mp:.1f}x higher throughput")
    print(f"Speedup vs TCP loopback sockets   : {speedup_tcp:.1f}x higher throughput")
    print("=" * 70)

if __name__ == "__main__":
    main()

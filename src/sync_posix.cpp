#if !defined(_WIN32)

#include "ipc/sync.hpp"
#include <cstdint>
#include <climits>
#include <cerrno>
#include <ctime>

#if defined(__linux__)
#include <sys/syscall.h>
#include <linux/futex.h>
#include <unistd.h>
#include <signal.h>

namespace ipc::detail {

static inline int futex_wait(volatile const void* uaddr, uint32_t val, uint32_t timeout_ms) {
    struct timespec ts{};
    struct timespec* pts = nullptr;
    if (timeout_ms != UINT32_MAX) {
        ts.tv_sec = timeout_ms / 1000;
        ts.tv_nsec = (timeout_ms % 1000) * 1000000L;
        pts = &ts;
    }
    // FUTEX_WAIT_PRIVATE operates on process-private memory, FUTEX_WAIT is inter-process
    return static_cast<int>(syscall(SYS_futex, uaddr, FUTEX_WAIT, val, pts, nullptr, 0));
}

static inline int futex_wake(volatile const void* uaddr, int count) {
    return static_cast<int>(syscall(SYS_futex, uaddr, FUTEX_WAKE, count, nullptr, nullptr, 0));
}

bool os_wait_u64(volatile const void* address, uint64_t compare_val, uint32_t timeout_ms) {
    // Monotonic sequences change in lower 32-bits on little-endian architectures
    auto* uaddr32 = reinterpret_cast<volatile const uint32_t*>(address);
    uint32_t val32 = static_cast<uint32_t>(compare_val & 0xFFFFFFFF);
    int res = futex_wait(uaddr32, val32, timeout_ms);
    if (res == -1 && errno == ETIMEDOUT) {
        return false;
    }
    return true;
}

bool os_wait_u32(volatile const void* address, uint32_t compare_val, uint32_t timeout_ms) {
    int res = futex_wait(address, compare_val, timeout_ms);
    if (res == -1 && errno == ETIMEDOUT) {
        return false;
    }
    return true;
}

void os_wake_single(volatile const void* address) {
    futex_wake(address, 1);
}

void os_wake_all(volatile const void* address) {
    futex_wake(address, INT_MAX);
}

uint32_t get_process_id() noexcept {
    return static_cast<uint32_t>(getpid());
}

bool is_process_alive(uint32_t pid) noexcept {
    if (pid == 0) return true;
    if (kill(static_cast<pid_t>(pid), 0) == 0) return true;
    return errno == EPERM;
}

} // namespace ipc::detail

#else // Generic POSIX fallback (e.g. macOS)

#include <thread>
#include <chrono>

namespace ipc::detail {

bool os_wait_u64(volatile const void* address, uint64_t compare_val, uint32_t timeout_ms) {
    auto start = std::chrono::steady_clock::now();
    auto* ptr = reinterpret_cast<volatile const uint64_t*>(address);
    while (*ptr == compare_val) {
        std::this_thread::yield();
        if (timeout_ms != UINT32_MAX) {
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start).count();
            if (elapsed >= timeout_ms) return false;
        }
    }
    return true;
}

bool os_wait_u32(volatile const void* address, uint32_t compare_val, uint32_t timeout_ms) {
    auto start = std::chrono::steady_clock::now();
    auto* ptr = reinterpret_cast<volatile const uint32_t*>(address);
    while (*ptr == compare_val) {
        std::this_thread::yield();
        if (timeout_ms != UINT32_MAX) {
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start).count();
            if (elapsed >= timeout_ms) return false;
        }
    }
    return true;
}

void os_wake_single(volatile const void* /*address*/) {}
void os_wake_all(volatile const void* /*address*/) {}

uint32_t get_process_id() noexcept {
    return 0;
}

bool is_process_alive(uint32_t /*pid*/) noexcept {
    return true;
}

} // namespace ipc::detail

#endif

#endif // !_WIN32

#pragma once

#include <cstdint>
#include <atomic>
#include <chrono>

#if defined(_MSC_VER) || defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#define IPC_CPU_PAUSE() _mm_pause()
#else
#define IPC_CPU_PAUSE() ((void)0)
#endif

namespace ipc {

namespace detail {
    // OS-specific primitive declarations implemented in sync_win32.cpp and sync_posix.cpp
    bool os_wait_u64(volatile const void* address, uint64_t compare_val, uint32_t timeout_ms);
    bool os_wait_u32(volatile const void* address, uint32_t compare_val, uint32_t timeout_ms);
    void os_wake_single(volatile const void* address);
    void os_wake_all(volatile const void* address);
    uint32_t get_process_id() noexcept;
    bool is_process_alive(uint32_t pid) noexcept;
} // namespace detail

class SyncWait {
public:
    static inline void cpu_pause() noexcept {
        IPC_CPU_PAUSE();
    }

    template <typename T>
    static bool wait_change(const std::atomic<T>& target, T current_val, uint32_t spin_count = 2048, uint32_t timeout_ms = 1000) noexcept {
        // Phase 1: Low-latency spin-wait with hardware pause
        for (uint32_t i = 0; i < spin_count; ++i) {
            if (target.load(std::memory_order_relaxed) != current_val) {
                return true;
            }
            cpu_pause();
        }

        // Phase 2: OS kernel wait (futex on Linux, WaitOnAddress on Windows)
        if constexpr (sizeof(T) == sizeof(uint64_t)) {
            while (target.load(std::memory_order_relaxed) == current_val) {
                if (!detail::os_wait_u64(&target, static_cast<uint64_t>(current_val), timeout_ms)) {
                    // Check once more in case of spurious timeout/race
                    return target.load(std::memory_order_relaxed) != current_val;
                }
            }
            return true;
        } else if constexpr (sizeof(T) == sizeof(uint32_t)) {
            while (target.load(std::memory_order_relaxed) == current_val) {
                if (!detail::os_wait_u32(&target, static_cast<uint32_t>(current_val), timeout_ms)) {
                    return target.load(std::memory_order_relaxed) != current_val;
                }
            }
            return true;
        } else {
            // Generic fallback polling
            auto start = std::chrono::steady_clock::now();
            while (target.load(std::memory_order_relaxed) == current_val) {
                cpu_pause();
                auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
                if (elapsed >= timeout_ms) {
                    return false;
                }
            }
            return true;
        }
    }

    template <typename T>
    static void wake_one(std::atomic<T>& target) noexcept {
        detail::os_wake_single(&target);
    }

    template <typename T>
    static void wake_all(std::atomic<T>& target) noexcept {
        detail::os_wake_all(&target);
    }
};

} // namespace ipc

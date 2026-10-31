#if defined(_WIN32)

#include "ipc/sync.hpp"
#include <windows.h>

#pragma comment(lib, "Synchronization.lib")

namespace ipc::detail {

bool os_wait_u64(volatile const void* address, uint64_t compare_val, uint32_t timeout_ms) {
    BOOL ok = WaitOnAddress(
        const_cast<void*>(address),
        &compare_val,
        sizeof(uint64_t),
        timeout_ms
    );
    if (!ok) {
        DWORD err = GetLastError();
        if (err == ERROR_TIMEOUT) {
            return false;
        }
    }
    return true;
}

bool os_wait_u32(volatile const void* address, uint32_t compare_val, uint32_t timeout_ms) {
    BOOL ok = WaitOnAddress(
        const_cast<void*>(address),
        &compare_val,
        sizeof(uint32_t),
        timeout_ms
    );
    if (!ok) {
        DWORD err = GetLastError();
        if (err == ERROR_TIMEOUT) {
            return false;
        }
    }
    return true;
}

void os_wake_single(volatile const void* address) {
    WakeByAddressSingle(const_cast<void*>(address));
}

void os_wake_all(volatile const void* address) {
    WakeByAddressAll(const_cast<void*>(address));
}

} // namespace ipc::detail

#endif // _WIN32

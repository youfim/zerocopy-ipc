#if defined(_WIN32)

#include "ipc/shm.hpp"
#include <windows.h>
#include <system_error>
#include <string>

namespace ipc {

namespace {

std::wstring to_wide_str(const std::string& str) {
    if (str.empty()) return std::wstring();
    int size_needed = MultiByteToWideChar(CP_UTF8, 0, str.data(), static_cast<int>(str.size()), NULL, 0);
    std::wstring wstr(size_needed, 0);
    MultiByteToWideChar(CP_UTF8, 0, str.data(), static_cast<int>(str.size()), &wstr[0], size_needed);
    return wstr;
}

std::wstring format_shm_name(const std::string& name) {
    std::wstring wname = to_wide_str(name);
    if (wname.rfind(L"Local\\", 0) != 0 && wname.rfind(L"Global\\", 0) != 0) {
        return L"Local\\" + wname;
    }
    return wname;
}

class Win32SharedMemoryRegion final : public SharedMemoryRegion {
public:
    Win32SharedMemoryRegion(std::string name, HANDLE handle, void* ptr, size_t size, bool is_owner)
        : name_(std::move(name)), handle_(handle), ptr_(ptr), size_(size), is_owner_(is_owner) {}

    ~Win32SharedMemoryRegion() override {
        if (ptr_) {
            UnmapViewOfFile(ptr_);
            ptr_ = nullptr;
        }
        if (handle_) {
            CloseHandle(handle_);
            handle_ = nullptr;
        }
    }

    void* data() noexcept override { return ptr_; }
    const void* data() const noexcept override { return ptr_; }
    size_t size() const noexcept override { return size_; }
    const std::string& name() const noexcept override { return name_; }
    bool is_owner() const noexcept override { return is_owner_; }

private:
    std::string name_;
    HANDLE handle_ = nullptr;
    void* ptr_ = nullptr;
    size_t size_ = 0;
    bool is_owner_ = false;
};

} // namespace

std::unique_ptr<SharedMemoryRegion> SharedMemoryRegion::create(const std::string& name, size_t size) {
    std::wstring wname = format_shm_name(name);

    DWORD size_high = static_cast<DWORD>((static_cast<uint64_t>(size) >> 32) & 0xFFFFFFFF);
    DWORD size_low  = static_cast<DWORD>(static_cast<uint64_t>(size) & 0xFFFFFFFF);

    HANDLE hMap = CreateFileMappingW(
        INVALID_HANDLE_VALUE,
        NULL,
        PAGE_READWRITE,
        size_high,
        size_low,
        wname.c_str()
    );

    if (!hMap) {
        throw std::system_error(static_cast<int>(GetLastError()), std::system_category(), "CreateFileMappingW failed");
    }

    void* ptr = MapViewOfFile(hMap, FILE_MAP_ALL_ACCESS, 0, 0, size);
    if (!ptr) {
        DWORD err = GetLastError();
        CloseHandle(hMap);
        throw std::system_error(static_cast<int>(err), std::system_category(), "MapViewOfFile failed");
    }

    return std::make_unique<Win32SharedMemoryRegion>(name, hMap, ptr, size, true);
}

std::unique_ptr<SharedMemoryRegion> SharedMemoryRegion::open(const std::string& name) {
    std::wstring wname = format_shm_name(name);

    HANDLE hMap = OpenFileMappingW(FILE_MAP_ALL_ACCESS, FALSE, wname.c_str());
    if (!hMap) {
        throw std::system_error(static_cast<int>(GetLastError()), std::system_category(), "OpenFileMappingW failed");
    }

    void* ptr = MapViewOfFile(hMap, FILE_MAP_ALL_ACCESS, 0, 0, 0);
    if (!ptr) {
        DWORD err = GetLastError();
        CloseHandle(hMap);
        throw std::system_error(static_cast<int>(err), std::system_category(), "MapViewOfFile failed");
    }

    MEMORY_BASIC_INFORMATION mbi{};
    if (VirtualQuery(ptr, &mbi, sizeof(mbi)) == 0) {
        DWORD err = GetLastError();
        UnmapViewOfFile(ptr);
        CloseHandle(hMap);
        throw std::system_error(static_cast<int>(err), std::system_category(), "VirtualQuery failed");
    }

    return std::make_unique<Win32SharedMemoryRegion>(name, hMap, ptr, mbi.RegionSize, false);
}

} // namespace ipc

#endif // _WIN32

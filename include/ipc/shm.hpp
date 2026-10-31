#pragma once

#include <string>
#include <memory>
#include <cstddef>

namespace ipc {

class SharedMemoryRegion {
public:
    virtual ~SharedMemoryRegion() = default;

    SharedMemoryRegion(const SharedMemoryRegion&) = delete;
    SharedMemoryRegion& operator=(const SharedMemoryRegion&) = delete;

    [[nodiscard]] virtual void* data() noexcept = 0;
    [[nodiscard]] virtual const void* data() const noexcept = 0;
    [[nodiscard]] virtual size_t size() const noexcept = 0;
    [[nodiscard]] virtual const std::string& name() const noexcept = 0;
    [[nodiscard]] virtual bool is_owner() const noexcept = 0;

    static std::unique_ptr<SharedMemoryRegion> create(const std::string& name, size_t size);
    static std::unique_ptr<SharedMemoryRegion> open(const std::string& name);

protected:
    SharedMemoryRegion() = default;
};

} // namespace ipc

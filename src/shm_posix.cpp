#if !defined(_WIN32)

#include "ipc/shm.hpp"
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <system_error>
#include <cerrno>

namespace ipc {

namespace {

std::string format_posix_shm_name(const std::string& name) {
    if (name.empty() || name[0] != '/') {
        return "/" + name;
    }
    return name;
}

class PosixSharedMemoryRegion final : public SharedMemoryRegion {
public:
    PosixSharedMemoryRegion(std::string name, int fd, void* ptr, size_t size, bool is_owner)
        : name_(std::move(name)), fd_(fd), ptr_(ptr), size_(size), is_owner_(is_owner) {}

    ~PosixSharedMemoryRegion() override {
        if (ptr_ && ptr_ != MAP_FAILED) {
            munmap(ptr_, size_);
            ptr_ = nullptr;
        }
        if (fd_ != -1) {
            close(fd_);
            fd_ = -1;
        }
        if (is_owner_) {
            shm_unlink(format_posix_shm_name(name_).c_str());
        }
    }

    void* data() noexcept override { return ptr_; }
    const void* data() const noexcept override { return ptr_; }
    size_t size() const noexcept override { return size_; }
    const std::string& name() const noexcept override { return name_; }
    bool is_owner() const noexcept override { return is_owner_; }

private:
    std::string name_;
    int fd_ = -1;
    void* ptr_ = nullptr;
    size_t size_ = 0;
    bool is_owner_ = false;
};

} // namespace

std::unique_ptr<SharedMemoryRegion> SharedMemoryRegion::create(const std::string& name, size_t size) {
    std::string shm_name = format_posix_shm_name(name);

    // Unlink any preexisting segment with the same name
    shm_unlink(shm_name.c_str());

    int fd = shm_open(shm_name.c_str(), O_CREAT | O_RDWR | O_EXCL, 0666);
    if (fd == -1) {
        throw std::system_error(errno, std::generic_category(), "shm_open create failed");
    }

    if (ftruncate(fd, static_cast<off_t>(size)) == -1) {
        int err = errno;
        close(fd);
        shm_unlink(shm_name.c_str());
        throw std::system_error(err, std::generic_category(), "ftruncate failed");
    }

    void* ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (ptr == MAP_FAILED) {
        int err = errno;
        close(fd);
        shm_unlink(shm_name.c_str());
        throw std::system_error(err, std::generic_category(), "mmap failed");
    }

    return std::make_unique<PosixSharedMemoryRegion>(name, fd, ptr, size, true);
}

std::unique_ptr<SharedMemoryRegion> SharedMemoryRegion::open(const std::string& name) {
    std::string shm_name = format_posix_shm_name(name);

    int fd = shm_open(shm_name.c_str(), O_RDWR, 0666);
    if (fd == -1) {
        throw std::system_error(errno, std::generic_category(), "shm_open open failed");
    }

    struct stat sb{};
    if (fstat(fd, &sb) == -1) {
        int err = errno;
        close(fd);
        throw std::system_error(err, std::generic_category(), "fstat failed");
    }

    size_t size = static_cast<size_t>(sb.st_size);
    void* ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (ptr == MAP_FAILED) {
        int err = errno;
        close(fd);
        throw std::system_error(err, std::generic_category(), "mmap failed");
    }

    return std::make_unique<PosixSharedMemoryRegion>(name, fd, ptr, size, false);
}

} // namespace ipc

#endif // !_WIN32

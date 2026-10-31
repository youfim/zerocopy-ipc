#pragma once

#include <cstdint>
#include <atomic>
#include <cstddef>

namespace ipc {

inline constexpr uint64_t kMagicNumber = 0x5A43504332304950ULL; // "ZCPC20IP"
inline constexpr uint32_t kCurrentVersion = 1;
inline constexpr size_t kMaxTensorDims = 8;
inline constexpr size_t kCacheLineSize = 128; // Covers hardware destructive interference & L2 prefetch

enum class DataType : uint32_t {
    RawBytes = 0,
    UInt8 = 1,
    Int8 = 2,
    UInt16 = 3,
    Int16 = 4,
    UInt32 = 5,
    Int32 = 6,
    UInt64 = 7,
    Int64 = 8,
    Float32 = 9,
    Float64 = 10
};

enum SlotFlags : uint32_t {
    SlotFlag_None = 0,
    SlotFlag_EOS  = 1 << 0
};

enum ShmFlags : uint32_t {
    ShmFlag_None      = 0,
    ShmFlag_Shutdown  = 1 << 0
};

#pragma pack(push, 8)
struct SlotDescriptor {
    uint64_t sequence_id;
    uint64_t data_offset;
    uint64_t data_size;
    uint32_t data_type;
    uint32_t ndim;
    int64_t  shape[kMaxTensorDims];
    int64_t  strides[kMaxTensorDims];
    uint32_t flags;
    uint32_t reserved;
};
#pragma pack(pop)

static_assert(sizeof(SlotDescriptor) % 8 == 0, "SlotDescriptor must be 8-byte aligned");

#if defined(_MSC_VER)
#pragma warning(push)
#pragma warning(disable: 4324) // structure was padded due to alignment specifier
#endif

struct SharedMemoryHeader {
    uint64_t magic;
    uint32_t version;
    uint32_t ring_capacity;        // Must be a power of two
    uint64_t data_arena_capacity;
    uint64_t total_shm_size;

    alignas(kCacheLineSize) std::atomic<uint64_t> head;
    alignas(kCacheLineSize) std::atomic<uint64_t> tail;
    alignas(kCacheLineSize) std::atomic<uint64_t> arena_head;
    alignas(kCacheLineSize) std::atomic<uint64_t> arena_tail;
    alignas(kCacheLineSize) std::atomic<uint32_t> producer_pid;
    alignas(kCacheLineSize) std::atomic<uint32_t> consumer_pid;
    alignas(kCacheLineSize) std::atomic<uint32_t> flags;
};

#if defined(_MSC_VER)
#pragma warning(pop)
#endif

static_assert(offsetof(SharedMemoryHeader, head) % kCacheLineSize == 0, "head must be cacheline-aligned");
static_assert(offsetof(SharedMemoryHeader, tail) % kCacheLineSize == 0, "tail must be cacheline-aligned");
static_assert(offsetof(SharedMemoryHeader, arena_head) % kCacheLineSize == 0, "arena_head must be cacheline-aligned");
static_assert(offsetof(SharedMemoryHeader, arena_tail) % kCacheLineSize == 0, "arena_tail must be cacheline-aligned");

} // namespace ipc

#pragma once

#include "ipc/protocol.hpp"
#include "ipc/shm.hpp"
#include "ipc/sync.hpp"

#include <cstdint>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <chrono>

namespace ipc {

inline constexpr size_t align_up(size_t val, size_t alignment) noexcept {
    return (val + alignment - 1) & ~(alignment - 1);
}

inline bool is_power_of_two(uint64_t val) noexcept {
    return val > 0 && (val & (val - 1)) == 0;
}

class SpscRingBuffer {
public:
    // Calculates total shared memory size required for header, slots, and data arena
    static size_t calculate_total_shm_size(uint32_t ring_capacity, uint64_t data_arena_capacity) {
        if (!is_power_of_two(ring_capacity)) {
            throw std::invalid_argument("ring_capacity must be a power of two");
        }
        size_t header_size = align_up(sizeof(SharedMemoryHeader), 128);
        size_t slots_size = align_up(ring_capacity * sizeof(SlotDescriptor), 128);
        size_t arena_size = align_up(data_arena_capacity, 128);
        return header_size + slots_size + arena_size;
    }

    // Producer factory: creates a new shared memory region and initializes layout
    static std::unique_ptr<SpscRingBuffer> create(const std::string& name, uint32_t ring_capacity, uint64_t data_arena_capacity) {
        if (!is_power_of_two(ring_capacity)) {
            throw std::invalid_argument("ring_capacity must be a power of two");
        }
        size_t total_size = calculate_total_shm_size(ring_capacity, data_arena_capacity);
        auto region = SharedMemoryRegion::create(name, total_size);

        auto* buf = new SpscRingBuffer(std::move(region), true);
        buf->init_header(ring_capacity, data_arena_capacity, total_size);
        return std::unique_ptr<SpscRingBuffer>(buf);
    }

    // Consumer factory: opens an existing shared memory region
    static std::unique_ptr<SpscRingBuffer> open(const std::string& name) {
        auto region = SharedMemoryRegion::open(name);
        auto* buf = new SpscRingBuffer(std::move(region), false);
        buf->validate_header();
        return std::unique_ptr<SpscRingBuffer>(buf);
    }

    ~SpscRingBuffer() {
        if (header_) {
            header_->flags.fetch_or(ShmFlag_Shutdown, std::memory_order_release);
            SyncWait::wake_all(header_->head);
            SyncWait::wake_all(header_->tail);
        }
    }

    SpscRingBuffer(const SpscRingBuffer&) = delete;
    SpscRingBuffer& operator=(const SpscRingBuffer&) = delete;

    [[nodiscard]] uint32_t capacity() const noexcept { return header_->ring_capacity; }
    [[nodiscard]] uint64_t arena_capacity() const noexcept { return header_->data_arena_capacity; }
    [[nodiscard]] bool is_producer() const noexcept { return is_producer_; }
    [[nodiscard]] bool is_shutdown() const noexcept {
        return (header_->flags.load(std::memory_order_relaxed) & ShmFlag_Shutdown) != 0;
    }

    void shutdown() noexcept {
        header_->flags.fetch_or(ShmFlag_Shutdown, std::memory_order_release);
        SyncWait::wake_all(header_->head);
        SyncWait::wake_all(header_->tail);
    }

    // --- PRODUCER API ---

    // Acquires contiguous memory in the data arena for zero-copy writing
    bool write_acquire(size_t payload_size, void*& out_buffer_ptr, uint64_t& out_slot_index, uint32_t spin_count = 2048, uint32_t timeout_ms = 1000) {
        if (!is_producer_) {
            throw std::logic_error("write_acquire called on consumer instance");
        }

        const size_t aligned_size = align_up(payload_size, 64);
        if (aligned_size > header_->data_arena_capacity) {
            throw std::invalid_argument("payload exceeds total arena capacity");
        }

        const uint32_t cap = header_->ring_capacity;
        const uint64_t current_head = header_->head.load(std::memory_order_relaxed);

        // 1. Wait for ring buffer slot availability (head - tail < cap)
        while ((current_head - header_->tail.load(std::memory_order_acquire)) >= cap) {
            if (is_shutdown()) return false;
            if (!SyncWait::wait_change(header_->tail, header_->tail.load(std::memory_order_relaxed), spin_count, timeout_ms)) {
                if (is_shutdown()) return false;
            }
        }

        // 2. Manage arena space: contiguous allocation with wrap-around check
        uint64_t arena_write_pos = header_->arena_head.load(std::memory_order_relaxed);
        uint64_t arena_read_pos = header_->arena_tail.load(std::memory_order_acquire);

        // If payload doesn't fit in remaining space towards end of arena, wrap to 0
        bool wrapped = false;
        if (arena_write_pos + aligned_size > header_->data_arena_capacity) {
            // Must wrap to beginning of arena
            // Ensure consumer has progressed past offset 0
            while (arena_read_pos <= aligned_size && arena_write_pos >= arena_read_pos && (header_->head.load(std::memory_order_relaxed) != header_->tail.load(std::memory_order_acquire))) {
                if (is_shutdown()) return false;
                if (!SyncWait::wait_change(header_->arena_tail, arena_read_pos, spin_count, timeout_ms)) {
                    if (is_shutdown()) return false;
                }
                arena_read_pos = header_->arena_tail.load(std::memory_order_acquire);
            }
            arena_write_pos = 0;
            wrapped = true;
        } else {
            // Fits linearly, ensure write does not lap read position
            while (arena_write_pos < arena_read_pos && (arena_write_pos + aligned_size) > arena_read_pos) {
                if (is_shutdown()) return false;
                if (!SyncWait::wait_change(header_->arena_tail, arena_read_pos, spin_count, timeout_ms)) {
                    if (is_shutdown()) return false;
                }
                arena_read_pos = header_->arena_tail.load(std::memory_order_acquire);
            }
        }

        // Allocate
        last_allocated_offset_ = arena_write_pos;
        last_allocated_size_ = aligned_size;
        out_buffer_ptr = arena_base_ + arena_write_pos;
        out_slot_index = current_head;
        return true;
    }

    // Commits written slot descriptor and publishes to consumer
    void write_commit(uint64_t slot_index, const SlotDescriptor& user_desc) {
        if (!is_producer_) {
            throw std::logic_error("write_commit called on consumer instance");
        }

        const uint32_t mask = header_->ring_capacity - 1;
        SlotDescriptor& slot = slots_[slot_index & mask];

        // Populate slot descriptor
        slot = user_desc;
        slot.sequence_id = slot_index;
        slot.data_offset = last_allocated_offset_;

        // Update arena write pointer
        header_->arena_head.store(last_allocated_offset_ + last_allocated_size_, std::memory_order_release);

        // Publish head sequence to consumer with release semantics
        header_->head.store(slot_index + 1, std::memory_order_release);

        // Wake consumer waiting on head
        SyncWait::wake_one(header_->head);
    }

    // Helper: write contiguous buffer in one step
    bool write(const void* data, size_t size, uint32_t data_type = static_cast<uint32_t>(DataType::RawBytes), uint32_t spin_count = 2048, uint32_t timeout_ms = 1000) {
        void* dest = nullptr;
        uint64_t slot_idx = 0;
        if (!write_acquire(size, dest, slot_idx, spin_count, timeout_ms)) {
            return false;
        }
        if (data && size > 0) {
            std::memcpy(dest, data, size);
        }
        SlotDescriptor desc{};
        desc.data_size = size;
        desc.data_type = data_type;
        write_commit(slot_idx, desc);
        return true;
    }

    // --- CONSUMER API ---

    // Acquires read lease on the next available slot
    bool read_acquire(SlotDescriptor& out_desc, const void*& out_buffer_ptr, uint32_t spin_count = 2048, uint32_t timeout_ms = 1000) {
        if (is_producer_) {
            throw std::logic_error("read_acquire called on producer instance");
        }
        if (has_active_lease_) {
            throw std::logic_error("read_acquire called while another lease is active; release previous frame first");
        }

        const uint64_t current_tail = header_->tail.load(std::memory_order_relaxed);

        // Wait for unread slots (head > tail)
        while (header_->head.load(std::memory_order_acquire) == current_tail) {
            if (is_shutdown()) return false;
            if (!SyncWait::wait_change(header_->head, current_tail, spin_count, timeout_ms)) {
                if (is_shutdown()) return false;
            }
        }

        const uint32_t mask = header_->ring_capacity - 1;
        const SlotDescriptor& slot = slots_[current_tail & mask];
        out_desc = slot;
        out_buffer_ptr = arena_base_ + slot.data_offset;

        active_lease_offset_ = slot.data_offset;
        active_lease_size_ = align_up(slot.data_size, 64);
        has_active_lease_ = true;
        return true;
    }

    // Releases read lease, advancing tail and freeing arena memory
    void read_release() {
        if (!is_producer_ && !has_active_lease_) {
            return;
        }

        const uint64_t current_tail = header_->tail.load(std::memory_order_relaxed);

        // Advance arena tail to free data space
        header_->arena_tail.store(active_lease_offset_ + active_lease_size_, std::memory_order_release);

        // Advance tail pointer
        header_->tail.store(current_tail + 1, std::memory_order_release);

        has_active_lease_ = false;

        // Wake producer waiting on tail or arena_tail
        SyncWait::wake_one(header_->tail);
        SyncWait::wake_one(header_->arena_tail);
    }

    // RAII Frame Lease for Consumer
    class ReadLease {
    public:
        ReadLease(SpscRingBuffer* parent, SlotDescriptor desc, const void* data) noexcept
            : parent_(parent), desc_(desc), data_(data), valid_(true) {}

        ~ReadLease() {
            release();
        }

        ReadLease(ReadLease&& other) noexcept
            : parent_(other.parent_), desc_(other.desc_), data_(other.data_), valid_(other.valid_) {
            other.valid_ = false;
        }

        ReadLease& operator=(ReadLease&& other) noexcept {
            if (this != &other) {
                release();
                parent_ = other.parent_;
                desc_ = other.desc_;
                data_ = other.data_;
                valid_ = other.valid_;
                other.valid_ = false;
            }
            return *this;
        }

        ReadLease(const ReadLease&) = delete;
        ReadLease& operator=(const ReadLease&) = delete;

        [[nodiscard]] bool is_valid() const noexcept { return valid_; }
        [[nodiscard]] const SlotDescriptor& descriptor() const noexcept { return desc_; }
        [[nodiscard]] const void* data() const noexcept { return data_; }
        [[nodiscard]] size_t size() const noexcept { return desc_.data_size; }

        void release() noexcept {
            if (valid_ && parent_) {
                parent_->read_release();
                valid_ = false;
            }
        }

    private:
        SpscRingBuffer* parent_ = nullptr;
        SlotDescriptor desc_{};
        const void* data_ = nullptr;
        bool valid_ = false;
    };

    // Obtains an RAII ReadLease
    std::unique_ptr<ReadLease> receive(uint32_t spin_count = 2048, uint32_t timeout_ms = 1000) {
        SlotDescriptor desc{};
        const void* ptr = nullptr;
        if (!read_acquire(desc, ptr, spin_count, timeout_ms)) {
            return nullptr;
        }
        return std::make_unique<ReadLease>(this, desc, ptr);
    }

private:
    explicit SpscRingBuffer(std::unique_ptr<SharedMemoryRegion> region, bool is_producer)
        : region_(std::move(region)), is_producer_(is_producer) {
        base_ptr_ = reinterpret_cast<uint8_t*>(region_->data());
        header_ = reinterpret_cast<SharedMemoryHeader*>(base_ptr_);
    }

    void init_header(uint32_t ring_capacity, uint64_t data_arena_capacity, size_t total_size) {
        std::memset(base_ptr_, 0, total_size);

        header_->magic = kMagicNumber;
        header_->version = kCurrentVersion;
        header_->ring_capacity = ring_capacity;
        header_->data_arena_capacity = data_arena_capacity;
        header_->total_shm_size = total_size;

        header_->head.store(0, std::memory_order_relaxed);
        header_->tail.store(0, std::memory_order_relaxed);
        header_->arena_head.store(0, std::memory_order_relaxed);
        header_->arena_tail.store(0, std::memory_order_relaxed);
        header_->flags.store(ShmFlag_None, std::memory_order_relaxed);

        setup_pointers();
    }

    void validate_header() {
        if (header_->magic != kMagicNumber) {
            throw std::runtime_error("Invalid shared memory magic number");
        }
        if (header_->version != kCurrentVersion) {
            throw std::runtime_error("Mismatched IPC protocol version");
        }
        setup_pointers();
    }

    void setup_pointers() {
        size_t header_offset = align_up(sizeof(SharedMemoryHeader), 128);
        slots_ = reinterpret_cast<SlotDescriptor*>(base_ptr_ + header_offset);

        size_t slots_size = align_up(header_->ring_capacity * sizeof(SlotDescriptor), 128);
        arena_base_ = base_ptr_ + header_offset + slots_size;
    }

    std::unique_ptr<SharedMemoryRegion> region_;
    bool is_producer_ = false;

    uint8_t* base_ptr_ = nullptr;
    SharedMemoryHeader* header_ = nullptr;
    SlotDescriptor* slots_ = nullptr;
    uint8_t* arena_base_ = nullptr;

    // Producer state
    uint64_t last_allocated_offset_ = 0;
    uint64_t last_allocated_size_ = 0;

    // Consumer state
    bool has_active_lease_ = false;
    uint64_t active_lease_offset_ = 0;
    uint64_t active_lease_size_ = 0;
};

} // namespace ipc

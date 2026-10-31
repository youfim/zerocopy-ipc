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
#include <thread>

#if defined(_WIN32)
#include <windows.h>
#endif

namespace ipc {

inline constexpr size_t align_up(size_t val, size_t alignment) noexcept {
    return (val + alignment - 1) & ~(alignment - 1);
}

inline bool is_power_of_two(uint64_t val) noexcept {
    return val > 0 && (val & (val - 1)) == 0;
}

class SpscRingBuffer {
public:
    static size_t calculate_total_shm_size(uint32_t ring_capacity, uint64_t data_arena_capacity) {
        if (!is_power_of_two(ring_capacity)) {
            throw std::invalid_argument("ring_capacity must be a power of two");
        }
        size_t header_size = align_up(sizeof(SharedMemoryHeader), 128);
        size_t slots_size = align_up(ring_capacity * sizeof(SlotDescriptor), 128);
        size_t arena_size = align_up(data_arena_capacity, 128);
        return header_size + slots_size + arena_size;
    }

    static std::unique_ptr<SpscRingBuffer> create(const std::string& name, uint32_t ring_capacity, uint64_t data_arena_capacity) {
        if (!is_power_of_two(ring_capacity)) {
            throw std::invalid_argument("ring_capacity must be a power of two");
        }
        size_t total_size = calculate_total_shm_size(ring_capacity, data_arena_capacity);
        auto region = SharedMemoryRegion::create(name, total_size);

        auto* buf = new SpscRingBuffer(std::move(region), true);
        buf->init_header(ring_capacity, data_arena_capacity, total_size);
        buf->init_sync(name);
        return std::unique_ptr<SpscRingBuffer>(buf);
    }

    static std::unique_ptr<SpscRingBuffer> open(const std::string& name, uint32_t wait_ready_ms = 5000) {
        auto start = std::chrono::steady_clock::now();
        std::unique_ptr<SharedMemoryRegion> region;
        while (true) {
            try {
                region = SharedMemoryRegion::open(name);
                break;
            } catch (...) {
                auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - start).count();
                if (elapsed >= wait_ready_ms) {
                    throw;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }
        }

        auto* buf = new SpscRingBuffer(std::move(region), false);
        while (buf->header_->magic != kMagicNumber) {
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start).count();
            if (elapsed >= wait_ready_ms) {
                delete buf;
                throw std::runtime_error("Timed out waiting for shared memory segment initialization");
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }

        buf->validate_header();
        buf->header_->consumer_pid.store(detail::get_process_id(), std::memory_order_release);
        buf->init_sync(name);
        return std::unique_ptr<SpscRingBuffer>(buf);
    }

    ~SpscRingBuffer() {
        shutdown();
        close_sync();
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
        if (header_) {
            header_->flags.fetch_or(ShmFlag_Shutdown, std::memory_order_release);
            wake_head();
            wake_tail();
        }
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
        const uint32_t mask = cap - 1;
        const uint64_t current_head = header_->head.load(std::memory_order_relaxed);

        // 1. Wait for ring buffer slot availability (head - tail < cap)
        uint64_t current_tail = header_->tail.load(std::memory_order_acquire);
        while ((current_head - current_tail) >= cap) {
            if (is_shutdown()) return false;
            if (!wait_tail(current_tail, spin_count, timeout_ms)) {
                if (is_shutdown()) return false;
                current_tail = header_->tail.load(std::memory_order_acquire);
                if ((current_head - current_tail) >= cap) return false;
            } else {
                current_tail = header_->tail.load(std::memory_order_acquire);
            }
        }

        // 2. Arena allocation with wraparound
        uint64_t arena_pos = header_->arena_head.load(std::memory_order_relaxed);

        while (true) {
            current_tail = header_->tail.load(std::memory_order_acquire);
            bool empty = (current_head == current_tail);
            uint64_t oldest_offset = empty ? UINT64_MAX : slots_[current_tail & mask].data_offset;

            if (arena_pos + aligned_size <= header_->data_arena_capacity) {
                // Linear fit
                if (empty || oldest_offset <= arena_pos || (arena_pos + aligned_size <= oldest_offset)) {
                    break;
                }
            } else {
                // Wraparound to beginning of arena
                if (empty || oldest_offset >= aligned_size) {
                    arena_pos = 0;
                    break;
                }
            }

            // Space exhausted: wait for consumer to advance tail
            if (is_shutdown()) return false;
            if (!wait_tail(current_tail, spin_count, timeout_ms)) {
                if (is_shutdown()) return false;
                return false;
            }
        }

        last_allocated_offset_ = arena_pos;
        last_allocated_size_ = aligned_size;
        out_buffer_ptr = arena_base_ + arena_pos;
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
        wake_head();
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

        uint64_t current_tail = header_->tail.load(std::memory_order_relaxed);

        // Wait for unread slots (head > tail)
        while (header_->head.load(std::memory_order_acquire) == current_tail) {
            if (is_shutdown()) return false;
            if (!wait_head(current_tail, spin_count, timeout_ms)) {
                if (is_shutdown()) return false;
                if (header_->head.load(std::memory_order_acquire) == current_tail) return false;
            }
        }

        const uint32_t mask = header_->ring_capacity - 1;
        const SlotDescriptor& slot = slots_[current_tail & mask];
        out_desc = slot;
        out_buffer_ptr = arena_base_ + slot.data_offset;

        has_active_lease_ = true;
        return true;
    }

    // Releases read lease, advancing tail and freeing arena memory
    void read_release() {
        if (!is_producer_ && !has_active_lease_) {
            return;
        }

        const uint64_t current_tail = header_->tail.load(std::memory_order_relaxed);

        // Advance tail pointer to release slot and free arena memory
        header_->tail.store(current_tail + 1, std::memory_order_release);
        has_active_lease_ = false;

        // Wake producer waiting on tail
        wake_tail();
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
        header_->producer_pid.store(detail::get_process_id(), std::memory_order_release);
        header_->consumer_pid.store(0, std::memory_order_relaxed);
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

    void init_sync(const std::string& name) {
#if defined(_WIN32)
        std::wstring wname(name.begin(), name.end());
        if (wname.rfind(L"Local\\", 0) != 0 && wname.rfind(L"Global\\", 0) != 0) {
            wname = L"Local\\" + wname;
        }
        std::wstring h_name = wname + L"_h";
        std::wstring t_name = wname + L"_t";
        if (is_producer_) {
            event_head_ = CreateEventW(NULL, FALSE, FALSE, h_name.c_str());
            event_tail_ = CreateEventW(NULL, FALSE, FALSE, t_name.c_str());
        } else {
            event_head_ = OpenEventW(SYNCHRONIZE | EVENT_MODIFY_STATE, FALSE, h_name.c_str());
            event_tail_ = OpenEventW(SYNCHRONIZE | EVENT_MODIFY_STATE, FALSE, t_name.c_str());
        }
#else
        (void)name;
#endif
    }

    void close_sync() noexcept {
#if defined(_WIN32)
        if (event_head_) { CloseHandle(event_head_); event_head_ = nullptr; }
        if (event_tail_) { CloseHandle(event_tail_); event_tail_ = nullptr; }
#endif
    }

    bool wait_tail(uint64_t current_tail, uint32_t spin_count, uint32_t timeout_ms) {
        for (uint32_t i = 0; i < spin_count; ++i) {
            if (header_->tail.load(std::memory_order_relaxed) != current_tail) return true;
            SyncWait::cpu_pause();
        }

        uint32_t cons_pid = header_->consumer_pid.load(std::memory_order_relaxed);
        if (cons_pid != 0 && !detail::is_process_alive(cons_pid)) {
            throw PeerDiedException("Consumer process (PID " + std::to_string(cons_pid) + ") died unexpectedly");
        }

#if defined(_WIN32)
        if (event_tail_) {
            DWORD res = WaitForSingleObject(event_tail_, timeout_ms);
            if (res == WAIT_TIMEOUT) {
                cons_pid = header_->consumer_pid.load(std::memory_order_relaxed);
                if (cons_pid != 0 && !detail::is_process_alive(cons_pid)) {
                    throw PeerDiedException("Consumer process (PID " + std::to_string(cons_pid) + ") died unexpectedly");
                }
            }
            return res == WAIT_OBJECT_0 || header_->tail.load(std::memory_order_relaxed) != current_tail;
        }
#endif
        bool ok = SyncWait::wait_change(header_->tail, current_tail, 0, timeout_ms);
        if (!ok) {
            cons_pid = header_->consumer_pid.load(std::memory_order_relaxed);
            if (cons_pid != 0 && !detail::is_process_alive(cons_pid)) {
                throw PeerDiedException("Consumer process (PID " + std::to_string(cons_pid) + ") died unexpectedly");
            }
        }
        return ok;
    }

    bool wait_head(uint64_t current_tail, uint32_t spin_count, uint32_t timeout_ms) {
        for (uint32_t i = 0; i < spin_count; ++i) {
            if (header_->head.load(std::memory_order_relaxed) != current_tail) return true;
            SyncWait::cpu_pause();
        }

        uint32_t prod_pid = header_->producer_pid.load(std::memory_order_relaxed);
        if (prod_pid != 0 && !detail::is_process_alive(prod_pid)) {
            throw PeerDiedException("Producer process (PID " + std::to_string(prod_pid) + ") died unexpectedly");
        }

#if defined(_WIN32)
        if (event_head_) {
            DWORD res = WaitForSingleObject(event_head_, timeout_ms);
            if (res == WAIT_TIMEOUT) {
                prod_pid = header_->producer_pid.load(std::memory_order_relaxed);
                if (prod_pid != 0 && !detail::is_process_alive(prod_pid)) {
                    throw PeerDiedException("Producer process (PID " + std::to_string(prod_pid) + ") died unexpectedly");
                }
            }
            return res == WAIT_OBJECT_0 || header_->head.load(std::memory_order_relaxed) != current_tail;
        }
#endif
        bool ok = SyncWait::wait_change(header_->head, current_tail, 0, timeout_ms);
        if (!ok) {
            prod_pid = header_->producer_pid.load(std::memory_order_relaxed);
            if (prod_pid != 0 && !detail::is_process_alive(prod_pid)) {
                throw PeerDiedException("Producer process (PID " + std::to_string(prod_pid) + ") died unexpectedly");
            }
        }
        return ok;
    }

    void wake_head() noexcept {
#if defined(_WIN32)
        if (event_head_) SetEvent(event_head_);
#endif
        SyncWait::wake_one(header_->head);
    }

    void wake_tail() noexcept {
#if defined(_WIN32)
        if (event_tail_) SetEvent(event_tail_);
#endif
        SyncWait::wake_one(header_->tail);
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

#if defined(_WIN32)
    HANDLE event_head_ = nullptr;
    HANDLE event_tail_ = nullptr;
#endif
};

} // namespace ipc

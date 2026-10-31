#include "ipc/ring_buffer.hpp"
#include <iostream>
#include <thread>
#include <vector>
#include <chrono>
#include <cassert>
#include <random>

#define TEST_ASSERT(cond, msg) \
    do { \
        if (!(cond)) { \
            std::cerr << "Assertion failed at line " << __LINE__ << ": " << msg << std::endl; \
            std::abort(); \
        } \
    } while (false)

void test_basic_create_open() {
    std::cout << "[Test] Running test_basic_create_open..." << std::endl;
    const std::string name = "test_shm_basic";
    const uint32_t ring_capacity = 64;
    const uint64_t arena_capacity = 1024 * 1024; // 1 MB

    auto producer = ipc::SpscRingBuffer::create(name, ring_capacity, arena_capacity);
    TEST_ASSERT(producer != nullptr, "Producer creation failed");
    TEST_ASSERT(producer->capacity() == ring_capacity, "Mismatched capacity");
    TEST_ASSERT(producer->arena_capacity() == arena_capacity, "Mismatched arena capacity");
    TEST_ASSERT(producer->is_producer(), "Should be producer");

    auto consumer = ipc::SpscRingBuffer::open(name);
    TEST_ASSERT(consumer != nullptr, "Consumer open failed");
    TEST_ASSERT(consumer->capacity() == ring_capacity, "Mismatched capacity");
    TEST_ASSERT(!consumer->is_producer(), "Should be consumer");

    std::cout << "[Test] test_basic_create_open PASSED." << std::endl;
}

void test_single_threaded_ping_pong() {
    std::cout << "[Test] Running test_single_threaded_ping_pong..." << std::endl;
    const std::string name = "test_shm_ping_pong";
    auto producer = ipc::SpscRingBuffer::create(name, 32, 256 * 1024);
    auto consumer = ipc::SpscRingBuffer::open(name);

    const int iterations = 1000;
    for (int i = 0; i < iterations; ++i) {
        uint64_t val = 0xDEADBEEF00000000ULL + i;
        bool ok_w = producer->write(&val, sizeof(val), static_cast<uint32_t>(ipc::DataType::UInt64));
        TEST_ASSERT(ok_w, "Producer write failed");

        auto lease = consumer->receive();
        TEST_ASSERT(lease != nullptr, "Consumer receive returned null");
        TEST_ASSERT(lease->size() == sizeof(val), "Mismatched payload size");
        TEST_ASSERT(lease->descriptor().data_type == static_cast<uint32_t>(ipc::DataType::UInt64), "Mismatched type");

        uint64_t read_val = *reinterpret_cast<const uint64_t*>(lease->data());
        TEST_ASSERT(read_val == val, "Value corrupted");
        lease->release();
    }

    std::cout << "[Test] test_single_threaded_ping_pong PASSED." << std::endl;
}

void test_multithreaded_high_throughput() {
    std::cout << "[Test] Running test_multithreaded_high_throughput (500,000 messages)..." << std::endl;
    const std::string name = "test_shm_throughput";
    const uint32_t capacity = 512;
    const uint64_t arena_size = 8 * 1024 * 1024; // 8 MB
    const uint64_t num_messages = 500000;

    auto producer = ipc::SpscRingBuffer::create(name, capacity, arena_size);
    auto consumer = ipc::SpscRingBuffer::open(name);

    auto start_time = std::chrono::high_resolution_clock::now();

    std::thread prod_thread([&]() {
        for (uint64_t i = 0; i < num_messages; ++i) {
            uint64_t payload[2] = { i, i * 7 };
            while (!producer->write(payload, sizeof(payload), static_cast<uint32_t>(ipc::DataType::UInt64), 1024, 100)) {
                if (producer->is_shutdown()) return;
            }
        }
    });

    std::thread cons_thread([&]() {
        for (uint64_t i = 0; i < num_messages; ++i) {
            auto lease = consumer->receive(1024, 100);
            TEST_ASSERT(lease != nullptr, "Consumer timed out");
            const auto* data = reinterpret_cast<const uint64_t*>(lease->data());
            TEST_ASSERT(data[0] == i, "Sequence mismatch");
            TEST_ASSERT(data[1] == i * 7, "Payload value mismatch");
            lease->release();
        }
    });

    prod_thread.join();
    cons_thread.join();

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed_sec = std::chrono::duration<double>(end_time - start_time).count();
    double msg_rate = static_cast<double>(num_messages) / elapsed_sec;

    std::cout << "[Test] Transferred " << num_messages << " messages in "
              << elapsed_sec << " s (" << static_cast<uint64_t>(msg_rate) << " msg/sec)" << std::endl;
    std::cout << "[Test] test_multithreaded_high_throughput PASSED." << std::endl;
}

void test_variable_size_wraparound() {
    std::cout << "[Test] Running test_variable_size_wraparound..." << std::endl;
    const std::string name = "test_shm_wrap";
    // Sized intentionally small to force hundreds of arena wraps
    const uint32_t capacity = 16;
    const uint64_t arena_size = 64 * 1024; // 64 KB
    const int num_frames = 5000;

    auto producer = ipc::SpscRingBuffer::create(name, capacity, arena_size);
    auto consumer = ipc::SpscRingBuffer::open(name);

    std::thread prod_thread([&]() {
        std::mt19937 rng(42);
        std::uniform_int_distribution<size_t> size_dist(128, 4096);

        std::vector<uint8_t> buffer;
        for (int i = 0; i < num_frames; ++i) {
            size_t sz = size_dist(rng);
            buffer.resize(sz);
            // Fill with deterministic pattern
            uint8_t pattern = static_cast<uint8_t>(i & 0xFF);
            std::fill(buffer.begin(), buffer.end(), pattern);

            while (!producer->write(buffer.data(), buffer.size(), static_cast<uint32_t>(ipc::DataType::RawBytes), 512, 100)) {
                if (producer->is_shutdown()) return;
            }
        }
    });

    std::thread cons_thread([&]() {
        for (int i = 0; i < num_frames; ++i) {
            auto lease = consumer->receive(512, 100);
            TEST_ASSERT(lease != nullptr, "Consumer timed out waiting for frame");
            uint8_t expected_pattern = static_cast<uint8_t>(i & 0xFF);
            const auto* ptr = reinterpret_cast<const uint8_t*>(lease->data());
            size_t sz = lease->size();

            // Verify content integrity
            for (size_t b = 0; b < sz; ++b) {
                if (ptr[b] != expected_pattern) {
                    TEST_ASSERT(false, "Data corruption detected during arena wraparound!");
                }
            }
            lease->release();
        }
    });

    prod_thread.join();
    cons_thread.join();

    std::cout << "[Test] test_variable_size_wraparound PASSED." << std::endl;
}

int main() {
    std::cout << "========================================" << std::endl;
    std::cout << "Starting SpscRingBuffer C++ Test Suite  " << std::endl;
    std::cout << "========================================" << std::endl;

    try {
        test_basic_create_open();
        test_single_threaded_ping_pong();
        test_multithreaded_high_throughput();
        test_variable_size_wraparound();
    } catch (const std::exception& ex) {
        std::cerr << "EXCEPTION: " << ex.what() << std::endl;
        return 1;
    }

    std::cout << "========================================" << std::endl;
    std::cout << "All C++ Unit Tests PASSED Successfully! " << std::endl;
    std::cout << "========================================" << std::endl;
    return 0;
}

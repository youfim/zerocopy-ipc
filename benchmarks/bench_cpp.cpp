#include "ipc/ring_buffer.hpp"
#include <iostream>
#include <thread>
#include <vector>
#include <chrono>
#include <algorithm>
#include <numeric>
#include <iomanip>

struct LatencyStats {
    double min_us;
    double p50_us;
    double p90_us;
    double p99_us;
    double p999_us;
    double max_us;
    double avg_us;
};

LatencyStats compute_stats(std::vector<double>& latencies) {
    if (latencies.empty()) return {};
    std::sort(latencies.begin(), latencies.end());
    size_t n = latencies.size();
    double sum = std::accumulate(latencies.begin(), latencies.end(), 0.0);

    return {
        latencies.front(),
        latencies[static_cast<size_t>(n * 0.50)],
        latencies[static_cast<size_t>(n * 0.90)],
        latencies[static_cast<size_t>(n * 0.99)],
        latencies[static_cast<size_t>(n * 0.999)],
        latencies.back(),
        sum / n
    };
}

void benchmark_throughput(size_t payload_size, uint64_t num_messages) {
    const std::string name = "bench_throughput_shm";
    const uint32_t capacity = 1024;
    const uint64_t arena_size = 128 * 1024 * 1024; // 128 MB

    auto producer = ipc::SpscRingBuffer::create(name, capacity, arena_size);
    auto consumer = ipc::SpscRingBuffer::open(name);

    std::vector<uint8_t> payload(payload_size, 0xAA);

    auto start = std::chrono::high_resolution_clock::now();

    std::thread prod([&]() {
        for (uint64_t i = 0; i < num_messages; ++i) {
            while (!producer->write(payload.data(), payload.size(), 0, 4096, 500)) {
                if (producer->is_shutdown()) return;
            }
        }
    });

    std::thread cons([&]() {
        for (uint64_t i = 0; i < num_messages; ++i) {
            auto lease = consumer->receive(4096, 500);
            if (!lease) break;
            lease->release();
        }
    });

    prod.join();
    cons.join();

    auto end = std::chrono::high_resolution_clock::now();
    double sec = std::chrono::duration<double>(end - start).count();
    double msg_rate = static_cast<double>(num_messages) / sec;
    double mb_rate = (static_cast<double>(num_messages * payload_size) / (1024.0 * 1024.0)) / sec;

    std::cout << std::setw(12) << payload_size << " B | "
              << std::setw(12) << static_cast<uint64_t>(msg_rate) << " msg/s | "
              << std::setw(12) << std::fixed << std::setprecision(2) << mb_rate << " MB/s | "
              << std::setprecision(4) << sec << " s" << std::endl;
}

void benchmark_ping_pong_latency(uint64_t round_trips) {
    const std::string name_req = "bench_ping_req";
    const std::string name_rsp = "bench_ping_rsp";

    // Two channels for bi-directional round-trip
    auto p1_req = ipc::SpscRingBuffer::create(name_req, 128, 4 * 1024 * 1024);
    auto p2_rsp = ipc::SpscRingBuffer::create(name_rsp, 128, 4 * 1024 * 1024);

    auto c2_req = ipc::SpscRingBuffer::open(name_req);
    auto c1_rsp = ipc::SpscRingBuffer::open(name_rsp);

    std::vector<double> latencies;
    latencies.reserve(round_trips);

    // Responder thread
    std::thread responder([&]() {
        uint64_t val = 0;
        for (uint64_t i = 0; i < round_trips; ++i) {
            auto lease = c2_req->receive(4096, 1000);
            if (!lease) break;
            val = *reinterpret_cast<const uint64_t*>(lease->data());
            lease->release();

            p2_rsp->write(&val, sizeof(val), 0, 4096, 1000);
        }
    });

    // Requester (measures RTT)
    for (uint64_t i = 0; i < round_trips; ++i) {
        auto t0 = std::chrono::high_resolution_clock::now();
        p1_req->write(&i, sizeof(i), 0, 4096, 1000);

        auto lease = c1_rsp->receive(4096, 1000);
        auto t1 = std::chrono::high_resolution_clock::now();
        if (lease) lease->release();

        double us = std::chrono::duration<double, std::micro>(t1 - t0).count();
        latencies.push_back(us);
    }

    responder.join();

    auto stats = compute_stats(latencies);
    std::cout << "\n--- Round-Trip Latency (RTT) Stats over " << round_trips << " samples ---" << std::endl;
    std::cout << "Avg:   " << std::setw(8) << stats.avg_us   << " us\n"
              << "Min:   " << std::setw(8) << stats.min_us   << " us\n"
              << "p50:   " << std::setw(8) << stats.p50_us   << " us\n"
              << "p90:   " << std::setw(8) << stats.p90_us   << " us\n"
              << "p99:   " << std::setw(8) << stats.p99_us   << " us\n"
              << "p99.9: " << std::setw(8) << stats.p999_us  << " us\n"
              << "Max:   " << std::setw(8) << stats.max_us   << " us" << std::endl;
}

int main() {
    std::cout << "========================================================\n"
              << "      Zero-Copy Shared Memory IPC C++ Benchmark         \n"
              << "========================================================\n\n";

    std::cout << "--- Throughput by Payload Size ---\n";
    std::cout << std::setw(12) << "Payload" << " | "
              << std::setw(12) << "Throughput" << " | "
              << std::setw(12) << "Bandwidth" << " | "
              << "Duration" << std::endl;
    std::cout << "--------------------------------------------------------\n";

    benchmark_throughput(64,       1000000);
    benchmark_throughput(1024,      500000);
    benchmark_throughput(64 * 1024,  50000);
    benchmark_throughput(1024*1024,   2000);

    benchmark_ping_pong_latency(100000);

    return 0;
}

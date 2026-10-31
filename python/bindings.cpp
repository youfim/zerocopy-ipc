#include "ipc/ring_buffer.hpp"
#include <nanobind/nanobind.h>
#include <nanobind/ndarray.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>
#include <nanobind/stl/unique_ptr.h>

#include <vector>
#include <stdexcept>
#include <memory>
#include <cstring>

namespace nb = nanobind;
using namespace nb::literals;

namespace {

ipc::DataType dlpack_dtype_to_ipc(nb::dlpack::dtype dt) {
    using code = nb::dlpack::dtype_code;
    if (dt.code == (uint8_t)code::UInt) {
        if (dt.bits == 8) return ipc::DataType::UInt8;
        if (dt.bits == 16) return ipc::DataType::UInt16;
        if (dt.bits == 32) return ipc::DataType::UInt32;
        if (dt.bits == 64) return ipc::DataType::UInt64;
    } else if (dt.code == (uint8_t)code::Int) {
        if (dt.bits == 8) return ipc::DataType::Int8;
        if (dt.bits == 16) return ipc::DataType::Int16;
        if (dt.bits == 32) return ipc::DataType::Int32;
        if (dt.bits == 64) return ipc::DataType::Int64;
    } else if (dt.code == (uint8_t)code::Float) {
        if (dt.bits == 32) return ipc::DataType::Float32;
        if (dt.bits == 64) return ipc::DataType::Float64;
    }
    return ipc::DataType::RawBytes;
}

nb::dlpack::dtype ipc_dtype_to_dlpack(ipc::DataType dt) {
    using code = nb::dlpack::dtype_code;
    switch (dt) {
        case ipc::DataType::UInt8:   return { (uint8_t)code::UInt, 8, 1 };
        case ipc::DataType::Int8:    return { (uint8_t)code::Int, 8, 1 };
        case ipc::DataType::UInt16:  return { (uint8_t)code::UInt, 16, 1 };
        case ipc::DataType::Int16:   return { (uint8_t)code::Int, 16, 1 };
        case ipc::DataType::UInt32:  return { (uint8_t)code::UInt, 32, 1 };
        case ipc::DataType::Int32:   return { (uint8_t)code::Int, 32, 1 };
        case ipc::DataType::UInt64:  return { (uint8_t)code::UInt, 64, 1 };
        case ipc::DataType::Int64:   return { (uint8_t)code::Int, 64, 1 };
        case ipc::DataType::Float32: return { (uint8_t)code::Float, 32, 1 };
        case ipc::DataType::Float64: return { (uint8_t)code::Float, 64, 1 };
        default:                     return { (uint8_t)code::UInt, 8, 1 };
    }
}

class PyFrameLease {
public:
    PyFrameLease(std::unique_ptr<ipc::SpscRingBuffer::ReadLease> lease)
        : lease_(std::move(lease)) {}

    [[nodiscard]] bool is_valid() const noexcept {
        return lease_ && lease_->is_valid();
    }

    [[nodiscard]] size_t size() const {
        check_valid();
        return lease_->size();
    }

    [[nodiscard]] uint32_t data_type() const {
        check_valid();
        return lease_->descriptor().data_type;
    }

    [[nodiscard]] uint64_t sequence_id() const {
        check_valid();
        return lease_->descriptor().sequence_id;
    }

    [[nodiscard]] uint32_t ndim() const {
        check_valid();
        return lease_->descriptor().ndim;
    }

    [[nodiscard]] std::vector<int64_t> shape() const {
        check_valid();
        const auto& desc = lease_->descriptor();
        return std::vector<int64_t>(desc.shape, desc.shape + desc.ndim);
    }

    [[nodiscard]] std::vector<int64_t> strides() const {
        check_valid();
        const auto& desc = lease_->descriptor();
        return std::vector<int64_t>(desc.strides, desc.strides + desc.ndim);
    }

    [[nodiscard]] uintptr_t data_ptr() const {
        check_valid();
        return reinterpret_cast<uintptr_t>(lease_->data());
    }

    nb::bytes to_bytes() const {
        check_valid();
        return nb::bytes(reinterpret_cast<const char*>(lease_->data()), lease_->size());
    }

    // Direct zero-copy ndarray referencing shared memory
    nb::object create_numpy_view(nb::handle owner) const {
        check_valid();
        const auto& desc = lease_->descriptor();
        auto dt = ipc_dtype_to_dlpack(static_cast<ipc::DataType>(desc.data_type));

        if (desc.ndim == 0) {
            // Flat 1D byte array
            size_t shape[1] = { desc.data_size };
            return nb::cast(nb::ndarray<nb::numpy>(
                const_cast<void*>(lease_->data()),
                1,
                shape,
                owner,
                nullptr,
                dt
            ));
        }

        std::vector<size_t> shape_u(desc.ndim);
        for (uint32_t i = 0; i < desc.ndim; ++i) {
            shape_u[i] = static_cast<size_t>(desc.shape[i]);
        }

        return nb::cast(nb::ndarray<nb::numpy>(
            const_cast<void*>(lease_->data()),
            desc.ndim,
            shape_u.data(),
            owner,
            desc.strides,
            dt
        ));
    }

    // Zero-copy Python memoryview
    nb::object to_memoryview() const {
        check_valid();
        PyObject* mv = PyMemoryView_FromMemory(
            const_cast<char*>(reinterpret_cast<const char*>(lease_->data())),
            static_cast<Py_ssize_t>(lease_->size()),
            PyBUF_READ
        );
        if (!mv) {
            throw nb::python_error();
        }
        return nb::steal(mv);
    }

    void release() {
        if (lease_) {
            lease_->release();
            lease_.reset();
        }
    }

private:
    void check_valid() const {
        if (!is_valid()) {
            throw std::runtime_error("Attempted to access expired or released FrameLease");
        }
    }

    std::unique_ptr<ipc::SpscRingBuffer::ReadLease> lease_;
};

class PyProducer {
public:
    PyProducer(const std::string& name, uint32_t ring_capacity = 128, uint64_t data_arena_capacity = 64 * 1024 * 1024) {
        buffer_ = ipc::SpscRingBuffer::create(name, ring_capacity, data_arena_capacity);
    }

    bool send_bytes(const nb::bytes& data, uint32_t spin_count = 2048, uint32_t timeout_ms = 1000) {
        return buffer_->write(data.data(), data.size(), static_cast<uint32_t>(ipc::DataType::RawBytes), spin_count, timeout_ms);
    }

    bool send_array(nb::ndarray<nb::ro, nb::c_contig> array, uint32_t spin_count = 2048, uint32_t timeout_ms = 1000) {
        size_t total_bytes = array.nbytes();
        void* dest = nullptr;
        uint64_t slot_idx = 0;

        if (!buffer_->write_acquire(total_bytes, dest, slot_idx, spin_count, timeout_ms)) {
            return false;
        }

        std::memcpy(dest, array.data(), total_bytes);

        ipc::SlotDescriptor desc{};
        desc.data_size = total_bytes;
        desc.data_type = static_cast<uint32_t>(dlpack_dtype_to_ipc(array.dtype()));
        desc.ndim = static_cast<uint32_t>(array.ndim());
        if (desc.ndim > ipc::kMaxTensorDims) {
            desc.ndim = static_cast<uint32_t>(ipc::kMaxTensorDims);
        }

        for (uint32_t i = 0; i < desc.ndim; ++i) {
            desc.shape[i] = static_cast<int64_t>(array.shape(i));
            desc.strides[i] = static_cast<int64_t>(array.stride(i));
        }

        buffer_->write_commit(slot_idx, desc);
        return true;
    }

    void shutdown() {
        if (buffer_) buffer_->shutdown();
    }

    [[nodiscard]] bool is_shutdown() const {
        return buffer_ ? buffer_->is_shutdown() : true;
    }

    [[nodiscard]] uint32_t capacity() const {
        return buffer_->capacity();
    }

    [[nodiscard]] uint64_t arena_capacity() const {
        return buffer_->arena_capacity();
    }

private:
    std::unique_ptr<ipc::SpscRingBuffer> buffer_;
};

class PyConsumer {
public:
    PyConsumer(const std::string& name, uint32_t wait_ready_ms = 5000) {
        buffer_ = ipc::SpscRingBuffer::open(name, wait_ready_ms);
    }

    std::unique_ptr<PyFrameLease> receive(uint32_t spin_count = 2048, uint32_t timeout_ms = 1000) {
        auto lease = buffer_->receive(spin_count, timeout_ms);
        if (!lease) {
            return nullptr;
        }
        return std::make_unique<PyFrameLease>(std::move(lease));
    }

    void shutdown() {
        if (buffer_) buffer_->shutdown();
    }

    [[nodiscard]] bool is_shutdown() const {
        return buffer_ ? buffer_->is_shutdown() : true;
    }

    [[nodiscard]] uint32_t capacity() const {
        return buffer_->capacity();
    }

    [[nodiscard]] uint64_t arena_capacity() const {
        return buffer_->arena_capacity();
    }

private:
    std::unique_ptr<ipc::SpscRingBuffer> buffer_;
};

} // namespace

NB_MODULE(_core, m) {
    m.doc() = "Ultra-low-latency zero-copy shared memory IPC nanobind module";

    nb::enum_<ipc::DataType>(m, "DataType")
        .value("RawBytes", ipc::DataType::RawBytes)
        .value("UInt8", ipc::DataType::UInt8)
        .value("Int8", ipc::DataType::Int8)
        .value("UInt16", ipc::DataType::UInt16)
        .value("Int16", ipc::DataType::Int16)
        .value("UInt32", ipc::DataType::UInt32)
        .value("Int32", ipc::DataType::Int32)
        .value("UInt64", ipc::DataType::UInt64)
        .value("Int64", ipc::DataType::Int64)
        .value("Float32", ipc::DataType::Float32)
        .value("Float64", ipc::DataType::Float64)
        .export_values();

    nb::exception<ipc::PeerDiedException>(m, "PeerDiedError", PyExc_RuntimeError);

    nb::class_<PyFrameLease>(m, "FrameLease")
        .def("is_valid", &PyFrameLease::is_valid)
        .def("size", &PyFrameLease::size)
        .def("data_type", &PyFrameLease::data_type)
        .def("sequence_id", &PyFrameLease::sequence_id)
        .def("ndim", &PyFrameLease::ndim)
        .def("shape", &PyFrameLease::shape)
        .def("strides", &PyFrameLease::strides)
        .def("data_ptr", &PyFrameLease::data_ptr)
        .def("to_bytes", &PyFrameLease::to_bytes)
        .def("to_numpy", [](nb::handle self) {
            auto& frame = nb::cast<PyFrameLease&>(self);
            return frame.create_numpy_view(self);
        })
        .def("to_memoryview", &PyFrameLease::to_memoryview)
        .def("release", &PyFrameLease::release)
        .def("__enter__", [](PyFrameLease* self) { return self; })
        .def("__exit__", [](PyFrameLease& self, nb::args) { self.release(); });

    nb::class_<PyProducer>(m, "Producer")
        .def(nb::init<const std::string&, uint32_t, uint64_t>(),
             "name"_a, "ring_capacity"_a = 128, "data_arena_capacity"_a = 64 * 1024 * 1024)
        .def("send_bytes", &PyProducer::send_bytes, "data"_a, "spin_count"_a = 2048, "timeout_ms"_a = 1000)
        .def("send_array", &PyProducer::send_array, "array"_a, "spin_count"_a = 2048, "timeout_ms"_a = 1000)
        .def("shutdown", &PyProducer::shutdown)
        .def("is_shutdown", &PyProducer::is_shutdown)
        .def("capacity", &PyProducer::capacity)
        .def("arena_capacity", &PyProducer::arena_capacity);

    nb::class_<PyConsumer>(m, "Consumer")
        .def(nb::init<const std::string&, uint32_t>(), "name"_a, "wait_ready_ms"_a = 5000)
        .def("receive", &PyConsumer::receive, "spin_count"_a = 2048, "timeout_ms"_a = 1000)
        .def("shutdown", &PyConsumer::shutdown)
        .def("is_shutdown", &PyConsumer::is_shutdown)
        .def("capacity", &PyConsumer::capacity)
        .def("arena_capacity", &PyConsumer::arena_capacity);
}

cmake_minimum_required(VERSION 3.20)
project(zerocopy_ipc VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Compiler-specific optimization and warning flags
if(MSVC)
    add_compile_options(/W4 /permissive- /utf-8 /Zc:preprocessor /O2 /Oi /Ot /GL)
    add_link_options(/LTCG)
    add_compile_definitions(NOMINMAX WIN32_LEAN_AND_MEAN _CRT_SECURE_NO_WARNINGS)
else()
    add_compile_options(-Wall -Wextra -Wpedantic -O3 -march=native -flto)
    add_link_options(-flto)
endif()

# Core static library
set(IPC_CORE_SOURCES "")
set(IPC_CORE_LIBS "")

if(WIN32)
    list(APPEND IPC_CORE_SOURCES
        src/shm_win32.cpp
        src/sync_win32.cpp
    )
    list(APPEND IPC_CORE_LIBS
        Synchronization.lib
    )
else()
    list(APPEND IPC_CORE_SOURCES
        src/shm_posix.cpp
        src/sync_posix.cpp
    )
    list(APPEND IPC_CORE_LIBS
        pthread
        rt
    )
endif()

add_library(ipc_core STATIC ${IPC_CORE_SOURCES})
target_include_directories(ipc_core PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_link_libraries(ipc_core PUBLIC ${IPC_CORE_LIBS})

# Options
option(BUILD_TESTS "Build C++ unit tests" ON)
option(BUILD_BENCHMARKS "Build C++ benchmarks" ON)
option(BUILD_PYTHON_BINDINGS "Build Python nanobind module" OFF)

# Python bindings (nanobind)
if(BUILD_PYTHON_BINDINGS)
    find_package(Python 3.9 REQUIRED COMPONENTS Interpreter Development.Module)
    if(NOT TARGET nanobind::nanobind)
        find_package(nanobind CONFIG QUIET)
        if(NOT nanobind_FOUND)
            execute_process(
                COMMAND "${Python_EXECUTABLE}" -m nanobind --cmake_dir
                OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE nanobind_ROOT
            )
            list(APPEND CMAKE_PREFIX_PATH "${nanobind_ROOT}")
            find_package(nanobind CONFIG REQUIRED)
        endif()
    endif()

    nanobind_add_module(_core NB_STATIC
        python/bindings.cpp
    )
    target_link_libraries(_core PRIVATE ipc_core)
    target_include_directories(_core PRIVATE include)

    # Output to the python package folder for in-tree execution
    set_target_properties(_core PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/python/zero_copy_ipc"
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/python/zero_copy_ipc"
    )
endif()

# Tests
if(BUILD_TESTS)
    enable_testing()
    add_executable(test_ring_buffer tests/test_ring_buffer.cpp)
    target_link_libraries(test_ring_buffer PRIVATE ipc_core)
    add_test(NAME ring_buffer_test COMMAND test_ring_buffer)
endif()

# Benchmarks
if(BUILD_BENCHMARKS)
    add_executable(bench_cpp benchmarks/bench_cpp.cpp)
    target_link_libraries(bench_cpp PRIVATE ipc_core)
endif()

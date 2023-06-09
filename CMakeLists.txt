cmake_minimum_required(VERSION 3.21)

project(MABPL)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "out/build")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0")

set(benchmark_DIR "libs/benchmark/build")
find_package(benchmark REQUIRED)

add_executable(${PROJECT_NAME}
        src/data_generation/data_generator.cpp
        src/library/select.cpp
        src/benchmarking/selectTests.cpp
        src/main.cpp
        src/utils/dataHelpers.cpp)

target_link_libraries(${PROJECT_NAME} benchmark::benchmark)

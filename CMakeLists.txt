cmake_minimum_required(VERSION 3.21)

project(MABPL)
enable_testing()
add_test(NAME MemoryCheck COMMAND valgrind --tool=memcheck --leak-check=yes ./out/build/MABPL)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "out/build")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -g -O0")


set(benchmark_DIR "libs/benchmark/build")
find_package(benchmark REQUIRED)

link_directories(/home/jack/CLionProjects/micro-adaptive-bulk-processing-library/libs/papi/src/install/lib)

add_executable(${PROJECT_NAME}
        src/data_generation/data_generator.cpp
        src/library/select.cpp
        src/benchmarking/selectTests.cpp
        src/main.cpp
        src/utils/dataHelpers.cpp
        src/perf_counters/hpcTests.cpp)

target_link_libraries(${PROJECT_NAME} benchmark::benchmark)

target_link_libraries(${PROJECT_NAME} papi)


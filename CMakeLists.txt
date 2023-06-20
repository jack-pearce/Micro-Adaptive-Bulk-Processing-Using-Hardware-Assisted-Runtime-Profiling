cmake_minimum_required(VERSION 3.21)

project(MABPL)
enable_testing()
add_test(NAME MemoryCheck COMMAND valgrind --tool=memcheck --leak-check=yes ./out/build/MABPL)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "out/build")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -g -O3 -march=native")


set(benchmark_DIR "libs/benchmark/build")
find_package(benchmark REQUIRED)

link_directories(/home/jack/CLionProjects/micro-adaptive-bulk-processing-library/libs/papi/src/install/lib)

#set(TBB_ROOT "/home/jack/CLionProjects/micro-adaptive-bulk-processing-library/libs/tbb/oneTBB")
#find_package(TBB REQUIRED)
#link_directories(/home/jack/CLionProjects/micro-adaptive-bulk-processing-library/libs/tbb/oneTBB/build/installationFolder/lib)

add_executable(${PROJECT_NAME}
        src/data_generation/dataGenerators.cpp
        src/data_generation/dataFiles.cpp
        src/library/select.cpp
        src/library/papi.cpp
        src/library/hardwareTuning.cpp
        src/time_benchmarking/timeBenchmarkSelect.cpp
        src/time_benchmarking/timeBenchmarkHelpers.cpp
        src/main.cpp
        src/utils/dataHelpers.cpp
        src/utils/papiHelpers.cpp
        src/perf_counters/counterBenchmark.cpp)

target_link_libraries(${PROJECT_NAME} benchmark::benchmark)

target_link_libraries(${PROJECT_NAME} papi)

#target_link_libraries(${PROJECT_NAME} tbb)
#target_link_libraries(${PROJECT_NAME} TBB::tbb)



cmake_minimum_required(VERSION 3.10.2)

project(MABPL)
#enable_testing()
#add_test(NAME MemoryCheck COMMAND valgrind --tool=memcheck --leak-check=yes ./out/build/MABPL)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -g -O3 -march=native -std=c++17")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -g -O3") # Required for folly (no -m flags)

set(benchmark_DIR "libs/benchmark/build")
find_package(benchmark REQUIRED)

link_directories("${CMAKE_SOURCE_DIR}/libs/papi/src/install/lib")

#set(FOLLY_DIR "libs//folly")
#find_package(folly REQUIRED)
#find_package(gflags CONFIG REQUIRED)

#find_package(absl REQUIRED)

add_subdirectory(libs/robin-map)

#add_subdirectory(libs/hopscotch-map)

find_package(PkgConfig REQUIRED)
pkg_check_modules(JSONCPP REQUIRED jsoncpp)
include_directories(${JSONCPP_INCLUDE_DIRS})

add_executable(${PROJECT_NAME}
        benchmarks/data_generation/dataFiles.cpp
        benchmarks/data_generation/dataFiles.h
        library/src/operators/select.cpp
        library/src/utilities/papi.cpp
        library/src/utilities/systemInformation.cpp
        benchmarks/main.cpp
        benchmarks/utilities/dataHelpers.cpp
        benchmarks/utilities/papiHelpers.cpp
        benchmarks/cycles_benchmarking/selectCyclesBenchmark.h
        benchmarks/data_generation/dataGenerators.cpp
        benchmarks/data_generation/dataGenerators.h
        library/src/operators/groupBy.cpp
        benchmarks/cycles_benchmarking/groupByCyclesBenchmark.h
        library/include/mabpl.h
        library/src/machine_constants/machineConstants.cpp
        benchmarks/time_benchmarking/timeBenchmarkHelpers.cpp
        benchmarks/time_benchmarking/selectTimeBenchmark.h
        benchmarks/main.h
        library/src/operators/partition.cpp
        benchmarks/cycles_benchmarking/partitionCyclesBenchmark.h)

target_link_libraries(${PROJECT_NAME} benchmark::benchmark)

target_link_libraries(${PROJECT_NAME} papi)

#target_link_libraries(${PROJECT_NAME} Folly::folly)

#target_link_libraries(${PROJECT_NAME} absl::base absl::hash absl::flat_hash_map)

target_link_libraries(${PROJECT_NAME} tsl::robin_map)

target_link_libraries(${PROJECT_NAME} ${JSONCPP_LIBRARIES})

set(benchmark_DIR "${CMAKE_SOURCE_DIR}/libs/benchmark/build")
find_package(benchmark REQUIRED)

link_directories("${CMAKE_SOURCE_DIR}/libs/papi/src/install/lib")

add_executable(hazard_adaptive_library_benchmark
        data_generation/dataFiles.cpp
        data_generation/dataFiles.h
        main.cpp
        utilities/dataHelpers.cpp
        utilities/papiHelpers.cpp
        cycles_benchmarking/selectCyclesBenchmark.h
        data_generation/dataGenerators.cpp
        data_generation/dataGenerators.h
        cycles_benchmarking/groupByCyclesBenchmark.h
        time_benchmarking/timeBenchmarkHelpers.cpp
        time_benchmarking/selectTimeBenchmark.h
        main.h
        cycles_benchmarking/partitionCyclesBenchmark.h
)

target_include_directories(hazard_adaptive_library_benchmark PUBLIC
        ${CMAKE_SOURCE_DIR}/libs/papi/src/install/include
        ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(hazard_adaptive_library_benchmark PRIVATE hazard_adaptive_library)
target_link_libraries(hazard_adaptive_library_benchmark PRIVATE benchmark::benchmark)
target_link_libraries(hazard_adaptive_library_benchmark PRIVATE papi)
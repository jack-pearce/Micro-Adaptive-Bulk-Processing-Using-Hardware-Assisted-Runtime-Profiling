set(EXE_NAME "hazard_adaptive_library_benchmark")
set(LIB_NAME "hazard_adaptive_library")

set(benchmark_DIR "${CMAKE_SOURCE_DIR}/libs/benchmark/build")
find_package(benchmark REQUIRED)

link_directories("${CMAKE_SOURCE_DIR}/libs/papi/src/install/lib")

add_executable(${EXE_NAME}
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

target_include_directories(${EXE_NAME} PUBLIC
        ${CMAKE_SOURCE_DIR}/libs/papi/src/install/include
        ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(${EXE_NAME} PRIVATE ${LIB_NAME})
target_link_libraries(${EXE_NAME} PRIVATE benchmark::benchmark)
target_link_libraries(${EXE_NAME} PRIVATE papi)
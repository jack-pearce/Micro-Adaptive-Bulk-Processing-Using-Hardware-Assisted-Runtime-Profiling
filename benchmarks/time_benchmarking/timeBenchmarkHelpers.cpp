#include <benchmark/benchmark.h>

#include "timeBenchmarkHelpers.h"

void runTimeBenchmark(int argc, char** argv) {
    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();
}
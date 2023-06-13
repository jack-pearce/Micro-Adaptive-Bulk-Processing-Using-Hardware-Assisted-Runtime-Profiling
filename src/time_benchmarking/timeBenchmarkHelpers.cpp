#include "timeBenchmarkHelpers.h"
#include "benchmark/benchmark.h"

void runTimeBenchmark(int argc, char** argv) {
    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();
}

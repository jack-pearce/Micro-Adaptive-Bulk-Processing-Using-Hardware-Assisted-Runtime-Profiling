#include <string>
#include <vector>
#include <iostream>
#include <memory>

#include "time_benchmarking/timeBenchmarkSelect.h"
#include "time_benchmarking/timeBenchmarkHelpers.h"
#include "data_generation/dataGenerators.h"
#include "utils/dataHelpers.h"
#include "library/select.h"
#include "library/papi.h"
#include "perf_counters/counterBenchmark.h"
#include "data_generation/dataFiles.h"
#include "utils/papiHelpers.h"



void dataDistributionTest(const DataFile& dataFile) {
    LoadedData::getInstance(dataFile.getFilepath(), dataFile.getNumElements()).getData();
    std::cout << dataFile.getNumElements() << " elements" << std::endl;
    displayDistribution(dataFile);
}

void selectFunctionalityTest(const DataFile& dataFile, SelectImplementation selectImplementation) {
    std::unique_ptr<int[]> selection(new int[dataFile.getNumElements()]);
    int* inputData = LoadedData::getInstance(dataFile.getFilepath(), dataFile.getNumElements()).getData();

    SelectFunctionPtr selectFunctionPtr;
    setSelectFuncPtr(selectFunctionPtr, selectImplementation);

    for (int i = 0; i <= 100; i += 10) {
        int selected = selectFunctionPtr(dataFile.getNumElements(), inputData, selection.get(), i);
        std::cout << i << "%: " << static_cast<float>(selected) / static_cast<float>(dataFile.getNumElements()) << std::endl;
    }
}

void runSelectTimeBenchmark(const DataFile& dataFile, SelectImplementation selectImplementation, int sensitivityStride) {
    selectTimeBenchmark(dataFile, selectImplementation, sensitivityStride);
    runTimeBenchmark(0, nullptr);
}

void runSelectTimeBenchmarkSetIterations(const DataFile& dataFile, SelectImplementation selectImplementation, int sensitivityStride, int iterations) {
    selectTimeBenchmarkSetIterations(dataFile, selectImplementation, sensitivityStride, iterations);
    runTimeBenchmark(0, nullptr);
}

void selectCountersBenchmarkConfiguration_1(const DataFile &dataFile, SelectImplementation selectImplementation,
                                            int sensitivityStride, int iterations) {
//    std::vector<std::string> benchmarkCounters = {"PERF_COUNT_HW_CPU_CYCLES"}; // Try adding in other CPU cycle counter
    std::vector<std::string> benchmarkCounters = {"INSTRUCTION_RETIRED"}; // Try adding in other CPU cycle counter

    selectCountersBenchmark(dataFile,
                            selectImplementation,
                            sensitivityStride,
                            iterations,
                            benchmarkCounters);
}

void selectCountersBenchmarkConfiguration_2(const DataFile &dataFile, SelectImplementation selectImplementation,
                                            int sensitivityStride, int iterations) {
    std::vector<std::string> benchmarkCounters = {"PERF_COUNT_HW_CPU_CYCLES",
                                                  "INSTRUCTION_RETIRED",
                                                  "UNHALTED_CORE_CYCLES"};
    selectCountersBenchmark(dataFile,
                            selectImplementation,
                            sensitivityStride,
                            iterations,
                            benchmarkCounters);
}

void selectCpuCyclesBenchmarkConfiguration_1(const DataFile &dataFile, SelectImplementation selectImplementation,
                                             int sensitivityStride, int iterations) {
    selectCpuCyclesBenchmark(dataFile,
                             selectImplementation,
                             sensitivityStride,
                             iterations);
}


int main(int argc, char** argv) {

    selectCpuCyclesBenchmarkConfiguration_1(DataFiles::uniformIntDistribution250mValues,
                                            SelectImplementation::Adaptive,
                                            1,
                                            10);

//    selectCountersBenchmarkConfiguration_1(DataFiles::uniformIntDistribution250mValues,
//                                         SelectImplementation::Branch,
//                                         1,
//                                         10);
//    selectCountersBenchmarkConfiguration_1(DataFiles::uniformIntDistribution250mValues,
//                                         SelectImplementation::Predication,
//                                         1,
//                                         10);
//    selectCountersBenchmarkConfiguration_1(DataFiles::uniformIntDistribution25kValues,
//                                         SelectImplementation::Adaptive,
//                                         25,
//                                         1);

/*    long_long *values;
    values = Counters::getInstance().readEventSet();
    std::cout << values[0] << ", " << values[1] << std::endl;

    std::vector<std::string> extraCounters = {"UNHALTED_CORE_CYCLES",
                                              "INSTRUCTION_RETIRED",
                                              "L1-DCACHE-LOAD-MISSES"};

    long_long *extraValues = Counters::getInstance().getEvents(extraCounters);
    std::cout << extraValues[0] << ", " << extraValues[1] << ", " << extraValues[2] << std::endl;

    Counters::getInstance().readEventSet();
    std::cout << values[0] << ", " << values[1] << std::endl;
    std::cout << extraValues[0] << ", " << extraValues[1] << ", " << extraValues[2] << std::endl;


    Counters::getInstance().readEventSet();
    std::cout << values[0] << ", " << values[1] << std::endl;
    std::cout << extraValues[0] << ", " << extraValues[1] << ", " << extraValues[2] << std::endl;

    Counters::getInstance().removeEvents(extraCounters);

    std::cout << "Extra counters removed" << std::endl;

    Counters::getInstance().readEventSet();
    std::cout << values[0] << ", " << values[1] << std::endl;
    std::cout << extraValues[0] << ", " << extraValues[1] << ", " << extraValues[2] << std::endl;

    Counters::getInstance().readEventSet();
    std::cout << values[0] << ", " << values[1] << std::endl;
    std::cout << extraValues[0] << ", " << extraValues[1] << ", " << extraValues[2] << std::endl;*/

}

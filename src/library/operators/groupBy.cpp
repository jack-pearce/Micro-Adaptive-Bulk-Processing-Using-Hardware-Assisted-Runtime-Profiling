#include <iostream>

#include "groupBy.h"


namespace MABPL {

std::string getGroupByName(GroupBy groupByImplementation) {
    switch (groupByImplementation) {
        case GroupBy::Hash:
            return "GroupBy_Hash";
        case GroupBy::Sort:
            return "GroupBy_Sort";
        case GroupBy::Adaptive:
            return "GroupBy_Adaptive";
        case GroupBy::AdaptiveParallel:
            return "GroupBy_Adaptive_Parallel";
        default:
            std::cout << "Invalid selection of 'GroupBy' implementation!" << std::endl;
            exit(1);
    }
}

void* groupByAdaptiveParallelAuxTest(void* arg) {
    auto *args = static_cast<GroupByArgs*>(arg);
    int threadId = args->threadId;

    int eventSet = PAPI_NULL;
    long_long counterValues[2] = {0};
    std::vector<std::string> counters = {"INSTRUCTIONS_RETIRED",
                                         "PERF_COUNT_HW_CACHE_MISSES"};
    createThreadEventSet(&eventSet, counters);

    readThreadEventSet(eventSet, 2, counterValues);

    if (threadId == 0) {
        auto inputAggregate = new int[1000 * 1000];
        for (int i = 0; i < 1000000; i++) {
            inputAggregate[i] = i + 1;
        }
    }

    std::cout << "(" << threadId << ") " << "Thread executing work... eventSet " << eventSet << std::endl;

    readThreadEventSet(eventSet, 2, counterValues);

    std::cout << "(" << threadId << ") " << "Instructions Retired: " << *counterValues << std::endl;
    std::cout << "(" << threadId << ") " << "Cache misses: " << *(counterValues + 1) << std::endl;

    destroyThreadEventSet(eventSet, counterValues);

    return nullptr;
}

}
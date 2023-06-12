#include <iostream>
#include <vector>
#include <string>
#include <memory>

#include "../data_generation/dataGenerators.h"
#include "../library/select.h"
#include "../time_benchmarking//timeBM_select.h"
#include "../utils/dataHelpers.h"
#include "../utils/papiHelpers.h"
#include "../../libs/papi/src/install/include/papi.h"
#include "../data_generation/dataFiles.h"


void counterTest_1() {
    int retval, EventSet=PAPI_NULL;
    long_long values[1];

/* Initialize the PAPI library */
    retval = PAPI_library_init(PAPI_VER_CURRENT);
    if (retval != PAPI_VER_CURRENT) {
        fprintf(stderr, "PAPI library init error!\n");
        exit(1);
    }

/* Get event code */
    int EventCode;
    retval = PAPI_event_name_to_code("INSTRUCTION_RETIRED",&EventCode);
    if (retval != PAPI_OK) {
        fprintf(stderr, "PAPI could not create event code!\n");
        exit(1);
    }

/* Create the Event Set */
    if (PAPI_create_eventset(&EventSet) != PAPI_OK) {
        fprintf(stderr, "PAPI could not create eventset!\n");
        exit(1);
    }

/* Add Events to our Event Set */
    retval = PAPI_add_event(EventSet, EventCode);
    if (retval != PAPI_OK) {
        fprintf(stderr, "Could not add total instructions to event set!\n");
        std::cout << "Error code: " << retval << std::endl;
        exit(1);
    }

/* Start counting events in the Event Set */
    if (PAPI_start(EventSet) != PAPI_OK)
        exit(1);


/* Read the counting events in the Event Set */
//    if (PAPI_read(EventSet, values) != PAPI_OK)
//        exit(1);
//
//    printf("After reading the counters: %lld\n",values[0]);

/* Reset the counting events in the Event Set */
//    if (PAPI_reset(EventSet) != PAPI_OK)
//        exit(1);

    int sum = 0;
    for (int i = 0; i < 10000; i++) {
        sum += i;
    }

/* Add the counters in the Event Set */
//    if (PAPI_accum(EventSet, values) != PAPI_OK)
//        exit(1);
//    printf("After adding the counters: %lld\n",values[0]);

/* Stop the counting of events in the Event Set */
    if (PAPI_stop(EventSet, values) != PAPI_OK)
        exit(1);

    printf("After stopping the counters: %lld\n",values[0]);

    // Cleanup
    PAPI_destroy_eventset(&EventSet);
    PAPI_shutdown();
}

void selectCounterTest() {
    std::string filePath = uniformInstDistribution250mValues;
    int numElements = 1000000000 / sizeof(int);
    int sensitivityStride = 5;
    int numTests = 1 + (100 / sensitivityStride);

//    generateUniformDistributionCSV(filePath, numElements);
    std::unique_ptr<int[]> inputData(new int[numElements]);
    std::unique_ptr<int[]> selection(new int[numElements]);
    loadDataToArray(filePath, inputData.get());

    std::vector<std::string> counters = {"INSTRUCTION_RETIRED",
                                         "BRANCH_INSTRUCTIONS_RETIRED",
                                         "MISPREDICTED_BRANCH_RETIRED",
                                         "LLC_MISSES",
                                         "PERF_COUNT_HW_CACHE_MISSES"};
    long_long counterValues[counters.size()];
    int eventSet = initialisePAPIandCreateEventSet(counters);

    std::vector<std::vector<long_long>> results(numTests, std::vector<long_long>(counters.size() + 1, 0));
    int count = 0;

    for (int i = 0; i <= 100; i += sensitivityStride) {
        if (PAPI_reset(eventSet) != PAPI_OK)
            exit(1);

        selectBranch(numElements, inputData.get(), selection.get(), i);

        if (PAPI_read(eventSet, counterValues) != PAPI_OK)
            exit(1);

        results[count][0] = static_cast<long_long>(i);
        for (int j = 0; j < static_cast<int>(counters.size()); ++j) {
            results[count][j + 1] = counterValues[j];
        }
        count++;
    }

    std::vector<std::string> headers(counters);
    headers.insert(headers.begin(), "Sensitivity");
    std::string outputFilePath = "/home/jack/CLionProjects/micro-adaptive-bulk-processing-library/data/output/";
    std::string outputFileName = "Sensitivity_Output";
    std::string outputFullFilePath = outputFilePath + outputFileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, outputFullFilePath);

    teardownPAPI(eventSet, counterValues);
}
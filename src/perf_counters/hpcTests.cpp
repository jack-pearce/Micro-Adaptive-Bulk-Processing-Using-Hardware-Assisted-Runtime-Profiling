#include <iostream>
#include <vector>
#include <string>

#include "../data_generation/data_generator.h"
#include "../library/select.h"
#include "../benchmarking/selectTests.h"
#include "../utils/dataHelpers.h"
#include "../../libs/papi/src/install/include/papi.h"


int initialisePAPIandCreateEventSet(std::vector<std::string>& hpcs) {
    int retval, EventCode, EventSet=PAPI_NULL;

/* Initialize the PAPI library */
    retval = PAPI_library_init(PAPI_VER_CURRENT);
    if (retval != PAPI_VER_CURRENT) {
        std::cerr << "PAPI library init error!" << std::endl;
        exit(1);
    }

/* Create the Event Set */
    if (PAPI_create_eventset(&EventSet) != PAPI_OK) {
        std::cerr << "PAPI could not create event set!" << std::endl;
        exit(1);
    }

    for (const std::string& hpc : hpcs) {
        /* Get event code */
        retval = PAPI_event_name_to_code(hpc.c_str(), &EventCode);
        if (retval != PAPI_OK) {
            std::cerr << "PAPI could not create event code!" << std::endl;
            exit(1);
        }

        /* Add Events to our Event Set */
        retval = PAPI_add_event(EventSet, EventCode);
        if (retval != PAPI_OK) {
            std::cerr << "Could not add '" << hpc << "' to event set!" << std::endl;
            std::cerr << "Error code: " << retval << std::endl;
            exit(1);
        }
    }

    return EventSet;
}

void teardownPAPI(int EventSet, long_long hpcValues[]) {
    if (PAPI_stop(EventSet, hpcValues) != PAPI_OK) {
        std::cerr << "PAPI could not stop counting events!" << std::endl;
        exit(1);
    }

    PAPI_destroy_eventset(&EventSet);
    PAPI_shutdown();
}

void printHpcValues(std::vector<std::string>& hpcs, long_long hpcValues[]) {
    int n = hpcs.size();
    for (int i = 0; i < n; ++i) {
        std::cout << hpcs[i] << ": " << hpcValues[i] << std::endl;
    }
}

void hpcTest_1() {
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

void hpcTest_2() {
        long_long start_cycles, end_cycles, start_usec, end_usec;
        int EventSet = PAPI_NULL;

        if (PAPI_library_init(PAPI_VER_CURRENT) != PAPI_VER_CURRENT)
        exit(1);

/* Gets the starting time in clock cycles */
        start_cycles = PAPI_get_real_cyc();

/* Gets the starting time in microseconds */
        start_usec = PAPI_get_real_usec();

/*Create an EventSet */
        if (PAPI_create_eventset(&EventSet) != PAPI_OK)
        exit(1);
/* Gets the ending time in clock cycles */
        end_cycles = PAPI_get_real_cyc();

/* Gets the ending time in microseconds */
        end_usec = PAPI_get_real_usec();

        printf("Wall clock cycles: %lld\n", end_cycles - start_cycles);
        printf("Wall clock time in microseconds: %lld\n", end_usec - start_usec);
}

void hpcSelectTester() {
    std::string filePath = "/home/jack/CLionProjects/micro-adaptive-bulk-processing-library/data/input/uniformIntDistribution.csv";
    int numElements = 4000000 / sizeof(int);
    int sensitivityStride = 5;
    int numTests = 1 + (100 / sensitivityStride);
    generateUniformDistributionCSV(filePath, numElements);

    std::vector<int> inputData;
    std::vector<int> selection;
    int elements = BM_selectSetup(filePath, inputData, selection);

    std::vector<std::string> hpcs = {"INSTRUCTION_RETIRED",
                                     "BRANCH_INSTRUCTIONS_RETIRED",
                                     "MISPREDICTED_BRANCH_RETIRED",
                                     "LLC_MISSES",
                                     "PERF_COUNT_HW_CACHE_MISSES"};
    long_long hpcValues[hpcs.size()];
    int EventSet = initialisePAPIandCreateEventSet(hpcs);

    if (PAPI_start(EventSet) != PAPI_OK) {
        std::cerr << "PAPI could not start counting events!" << std::endl;
        exit(1);
    }

    std::vector<std::vector<long_long>> results(numTests, std::vector<long_long>(hpcs.size() + 1, 0));
    int count = 0;

    for (int i = 0; i <= 100; i += sensitivityStride) {
        if (PAPI_reset(EventSet) != PAPI_OK)
            exit(1);

        selectBranch(elements, inputData, selection, i);

        if (PAPI_read(EventSet, hpcValues) != PAPI_OK)
            exit(1);

        results[count][0] = static_cast<long_long>(i);
        for (int j = 0; j < hpcs.size(); ++j) {
            results[count][j + 1] = hpcValues[j];
        }
        count++;
    }

    std::vector<std::string> headers(hpcs);
    headers.insert(headers.begin(), "Sensitivity");
    std::string outputFilePath = "/home/jack/CLionProjects/micro-adaptive-bulk-processing-library/data/output/";
    std::string outputFileName = "Sensitivity_Output";
    std::string outputFullFilePath = outputFilePath + outputFileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, outputFullFilePath);

    teardownPAPI(EventSet, hpcValues);
}
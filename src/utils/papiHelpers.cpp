#include <iostream>
#include <vector>
#include <string>
#include <memory>

#include "../data_generation/dataGenerators.h"
#include "../library/select.h"
#include "../time_benchmarking//timeBM_select.h"
#include "../utils/dataHelpers.h"
#include "../../libs/papi/src/install/include/papi.h"
#include "papiHelpers.h"

int initialisePAPIandCreateEventSet(std::vector<std::string>& counters) {
    int returnValue, eventCode, eventSet=PAPI_NULL;

/* Initialize the PAPI library */
    if (PAPI_is_initialized() == PAPI_NOT_INITED) {
        returnValue = PAPI_library_init(PAPI_VER_CURRENT);
        if (returnValue != PAPI_VER_CURRENT) {
            std::cerr << "PAPI library init error!" << std::endl;
            exit(1);
        }
    }

/* Create the Event Set */
    returnValue = PAPI_create_eventset(&eventSet);
    if (returnValue != PAPI_OK) {
        std::cerr << "PAPI could not create event set!" << std::endl;
        std::cerr << "Error code: " << returnValue << std::endl;
        exit(1);
    }

    for (const std::string& counter : counters) {
        /* Get event code */
        returnValue = PAPI_event_name_to_code(counter.c_str(), &eventCode);
        if (returnValue != PAPI_OK) {
            std::cerr << "PAPI could not create event code!" << std::endl;
            exit(1);
        }

        /* Add Events to our Event Set */
        returnValue = PAPI_add_event(eventSet, eventCode);
        if (returnValue != PAPI_OK) {
            std::cerr << "Could not add '" << counter << "' to event set!" << std::endl;
            std::cerr << "Error code: " << returnValue << std::endl;
            exit(1);
        }
    }

    if (PAPI_start(eventSet) != PAPI_OK) {
        std::cerr << "PAPI could not start event set!" << std::endl;
        exit(1);
    }

    std::cout << "Event set created" << std::endl;

    return eventSet;
}

void teardownPAPI(int eventSet, long_long counterValues[]) {
    int returnValue = PAPI_stop(eventSet, counterValues);
    if (returnValue != PAPI_OK) {
        std::cerr << "PAPI could not stop counting events!" << std::endl;
        std::cerr << "Error code: " << returnValue << std::endl;
        exit(1);
    }

    PAPI_destroy_eventset(&eventSet);
    PAPI_shutdown();
}

void printHpcValues(std::vector<std::string>& counters, long_long counterValues[]) {
    int n = static_cast<int>(counters.size());
    for (int i = 0; i < n; ++i) {
        std::cout << counters[i] << ": " << counterValues[i] << std::endl;
    }
}
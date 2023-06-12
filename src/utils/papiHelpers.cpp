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
    int returnValue, EventCode, EventSet=PAPI_NULL;

/* Initialize the PAPI library */
    returnValue = PAPI_library_init(PAPI_VER_CURRENT);
    if (returnValue != PAPI_VER_CURRENT) {
        std::cerr << "PAPI library init error!" << std::endl;
        exit(1);
    }

/* Create the Event Set */
    if (PAPI_create_eventset(&EventSet) != PAPI_OK) {
        std::cerr << "PAPI could not create event set!" << std::endl;
        exit(1);
    }

    for (const std::string& counter : counters) {
        /* Get event code */
        returnValue = PAPI_event_name_to_code(counter.c_str(), &EventCode);
        if (returnValue != PAPI_OK) {
            std::cerr << "PAPI could not create event code!" << std::endl;
            exit(1);
        }

        /* Add Events to our Event Set */
        returnValue = PAPI_add_event(EventSet, EventCode);
        if (returnValue != PAPI_OK) {
            std::cerr << "Could not add '" << counter << "' to event set!" << std::endl;
            std::cerr << "Error code: " << returnValue << std::endl;
            exit(1);
        }
    }

    return EventSet;
}

void teardownPAPI(int EventSet, long_long counterValues[]) {
    if (PAPI_stop(EventSet, counterValues) != PAPI_OK) {
        std::cerr << "PAPI could not stop counting events!" << std::endl;
        exit(1);
    }

    PAPI_destroy_eventset(&EventSet);
    PAPI_shutdown();
}

void printHpcValues(std::vector<std::string>& counters, long_long counterValues[]) {
    int n = static_cast<int>(counters.size());
    for (int i = 0; i < n; ++i) {
        std::cout << counters[i] << ": " << counterValues[i] << std::endl;
    }
}
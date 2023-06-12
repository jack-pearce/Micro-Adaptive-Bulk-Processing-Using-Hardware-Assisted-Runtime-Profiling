#include <iostream>
#include <vector>
#include <string>
#include <memory>

#include "../data_generation/dataGenerators.h"
#include "../library/select.h"
#include "../benchmarking/selectBenchmarks.h"
#include "../utils/dataHelpers.h"
#include "../../libs/papi/src/install/include/papi.h"
#include "papiHelpers.h"

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
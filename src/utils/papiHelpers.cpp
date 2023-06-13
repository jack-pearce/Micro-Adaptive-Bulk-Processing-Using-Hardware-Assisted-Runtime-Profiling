#include <iostream>
#include <vector>
#include <string>

#include "../utils/dataHelpers.h"
#include "papiHelpers.h"

int initialisePAPIandCreateEventSet(std::vector<std::string>& counters) {
    int returnValue, eventCode, status, eventSet=PAPI_NULL;

    if (PAPI_is_initialized() == PAPI_NOT_INITED) {
        returnValue = PAPI_library_init(PAPI_VER_CURRENT);
        if (returnValue != PAPI_VER_CURRENT) {
            std::cerr << "PAPI library init error!" << std::endl;
            exit(1);
        }
        std::cout << "PAPI initialised" << std::endl;
    }

    returnValue = PAPI_create_eventset(&eventSet);
    if (returnValue != PAPI_OK) {
        std::cerr << "PAPI could not create event set!" << std::endl;
        std::cerr << "Error code: " << returnValue << std::endl;
        exit(1);
    }

    for (const std::string& counter : counters) {
        returnValue = PAPI_event_name_to_code(counter.c_str(), &eventCode);
        if (returnValue != PAPI_OK) {
            std::cerr << "PAPI could not create event code!" << std::endl;
            exit(1);
        }

        returnValue = PAPI_add_event(eventSet, eventCode);
        if (returnValue != PAPI_OK) {
            std::cerr << "Could not add '" << counter << "' to event set!" << std::endl;
            std::cerr << "Error code: " << returnValue << std::endl;
            exit(1);
        }
    }

    PAPI_state(eventSet, &status);
    if (status != 1) {
        returnValue = PAPI_start(eventSet);
        if (returnValue != PAPI_OK) {
            std::cerr << "PAPI could not start event set!" << std::endl;
            std::cerr << "Error code: " << returnValue << std::endl;
            exit(1);
        }
        std::cout << "event set started" << std::endl;
    }

/*    std::cout << status << std::endl;

    long_long results[counters.size()];
    results[0] = -1;
    if (PAPI_read(eventSet, results) != PAPI_OK)
        exit(1);

    for (long long result : results) {
        std::cout << result << std::endl;
    }*/

    return eventSet;
}

void shutdownPAPI(int eventSet) {
    PAPI_destroy_eventset(&eventSet);

    if (PAPI_is_initialized() != PAPI_NOT_INITED)
        PAPI_shutdown();
}
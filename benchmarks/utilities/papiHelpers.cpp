#include <iostream>
#include <vector>
#include <string>

#include "papiHelpers.hpp"

int initialisePAPIandCreateEventSet(std::vector<std::string>& counters) {
    int returnValue, eventCode, eventSet=PAPI_NULL;

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

    for (const auto& counter : counters) {
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

    PAPI_start(eventSet);

    return eventSet;
}

void shutdownPAPI(int eventSet, long_long counterValues[]) {
    std::cout << "Stop eventset: " << eventSet << ", return value: " << PAPI_stop(eventSet, counterValues) << std::endl;
    std::cout << "Cleanup eventset: " << eventSet << ", return value: " << PAPI_cleanup_eventset(eventSet) << std::endl;
    std::cout << "Destroy eventset: " << eventSet << ", return value: " << PAPI_destroy_eventset(&eventSet) << std::endl;

    if (PAPI_is_initialized() != PAPI_NOT_INITED)
        PAPI_shutdown();

    std::cout << "PAPI shutdown" << std::endl;

}
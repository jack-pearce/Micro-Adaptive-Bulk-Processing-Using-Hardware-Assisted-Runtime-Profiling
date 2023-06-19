#include <iostream>
#include <algorithm>

#include "papi.h"

Counters& Counters::getInstance() {
    static Counters instance;
    return instance;
}

Counters::Counters() {
    eventSet=PAPI_NULL;
    std::vector<std::string> initialCounters = {"PERF_COUNT_HW_CPU_CYCLES"};

    if (PAPI_library_init(PAPI_VER_CURRENT) != PAPI_VER_CURRENT) {
        std::cerr << "PAPI library init error!" << std::endl;
        exit(1);
    }

    if (PAPI_create_eventset(&eventSet) != PAPI_OK) {
        std::cerr << "PAPI could not create event set!" << std::endl;
        exit(1);
    }

    addEvents(initialCounters);
    PAPI_start(eventSet);
}

Counters::~Counters() {
    PAPI_stop(eventSet, counterValues);
    PAPI_cleanup_eventset(eventSet);
    PAPI_destroy_eventset(&eventSet);
    PAPI_shutdown();
}

long_long *Counters::addEvents(std::vector<std::string>& counterNames) {
    int eventCode;
    PAPI_stop(eventSet, counterValues);

    for (const std::string& counter : counterNames) {
        if (__builtin_expect(PAPI_event_name_to_code(counter.c_str(), &eventCode) != PAPI_OK, false)) {
            std::cerr << "PAPI could not create event code!" << std::endl;
            exit(1);
        }

        if (__builtin_expect(PAPI_add_event(eventSet, eventCode) != PAPI_OK,0)) {
            std::cerr << "Could not add '" << counter << "' to event set!" << std::endl;
            exit(1);
        }
    }

    counters.insert(counters.end(), counterNames.begin(), counterNames.end());
    PAPI_start(eventSet);
    return &(counterValues[counters.size() - counterNames.size()]);
}

long_long *Counters::eventsAlreadyInSet(std::vector<std::string>& newCounterNames) {
    for (std::size_t i = 0; i <= counters.size(); ++i) {
        if (counters[i] == newCounterNames[0]) {
            bool found = true;

            for (std::size_t j = 1; j < newCounterNames.size(); ++j) {
                if (counters[i + j] != newCounterNames[j]) {
                    found = false;
                    break;
                }
            }

            if (found) {
                return &(counterValues[i]);
            }
        }
    }
    return nullptr;
}

long_long *Counters::getEvents(std::vector<std::string>& counterNames) {
    long_long *eventValues = eventsAlreadyInSet(counterNames);
    if (__builtin_expect(eventValues != nullptr, true))
        return eventValues;

    return addEvents(counterNames);
}

long_long *Counters::readEventSet() {
    for (size_t i = 1; i < counters.size(); ++i) {
        counterValues[i] = 0;
    }

    if (__builtin_expect(PAPI_accum(eventSet, counterValues) != PAPI_OK, false)) {
        std::cerr << "Could not read event set!" << std::endl;
        exit(1);
    }
    return counterValues;
}
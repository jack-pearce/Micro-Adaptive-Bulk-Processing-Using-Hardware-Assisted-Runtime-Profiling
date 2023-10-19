#ifndef MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_PAPIHELPERS_H
#define MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_PAPIHELPERS_H

#include <papi.h>

int initialisePAPIandCreateEventSet(std::vector<std::string>& counters);
void shutdownPAPI(int eventSet, long_long counterValues[]);

#endif //MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_PAPIHELPERS_H

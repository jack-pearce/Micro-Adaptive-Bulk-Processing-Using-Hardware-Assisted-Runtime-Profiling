#ifndef MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_PAPIHELPERS_H
#define MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_PAPIHELPERS_H

#include "../../libs/papi/src/install/include/papi.h"

int initialisePAPIandCreateEventSet(std::vector<std::string>& counters);
void shutdownPAPI(int eventSet);

#endif //MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_PAPIHELPERS_H

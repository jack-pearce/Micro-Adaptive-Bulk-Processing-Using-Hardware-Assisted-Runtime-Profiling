#ifndef MABPL_PAPIHELPERS_H
#define MABPL_PAPIHELPERS_H

#include "../../libs/papi/src/install/include/papi.h"

int initialisePAPIandCreateEventSet(std::vector<std::string>& counters);
void shutdownPAPI();
void shutdownPAPI(int eventSet);
void printHpcValues(std::vector<std::string>& counters, long_long counterValues[]);

#endif //MABPL_PAPIHELPERS_H

#ifndef MABPL_PAPIHELPERS_H
#define MABPL_PAPIHELPERS_H

int initialisePAPIandCreateEventSet(std::vector<std::string>& counters);
void teardownPAPI(int EventSet, long_long counterValues[]);
void printHpcValues(std::vector<std::string>& counters, long_long counterValues[]);

#endif //MABPL_PAPIHELPERS_H

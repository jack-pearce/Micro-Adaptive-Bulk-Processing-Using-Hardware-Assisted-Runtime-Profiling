#ifndef MABPL_PAPIHELPERS_H
#define MABPL_PAPIHELPERS_H

int initialisePAPIandCreateEventSet(std::vector<std::string>& hpcs);
void teardownPAPI(int EventSet, long_long hpcValues[]);
void printHpcValues(std::vector<std::string>& hpcs, long_long hpcValues[]);

#endif //MABPL_PAPIHELPERS_H

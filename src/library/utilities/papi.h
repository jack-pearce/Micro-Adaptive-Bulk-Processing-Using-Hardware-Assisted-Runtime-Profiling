#ifndef MABPL_PAPI_H
#define MABPL_PAPI_H

#include <vector>
#include <string>

#include "../../../libs/papi/src/install/include/papi.h"

class Counters {
public:
    static Counters& getInstance();
    long_long *getEvents(std::vector<std::string>& counterNames);
    long_long *readEventSet();
    Counters(const Counters&) = delete;
    void operator=(const Counters&) = delete;

private:
    int eventSet;
    std::vector<std::string> counters;
    long_long counterValues[20] = {0};
    long_long *addEvents(std::vector<std::string>& counterNames);
    long_long *eventsAlreadyInSet(std::vector<std::string>& counterNames);
    Counters();
    ~Counters();
};

#endif //MABPL_PAPI_H

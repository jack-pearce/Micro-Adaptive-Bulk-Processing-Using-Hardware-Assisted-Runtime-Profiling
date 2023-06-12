#ifndef MABPL_COUNTERBM_H
#define MABPL_COUNTERBM_H

#include "../library/select.h"

void selectCounterBM(SelectImplementation selectImplementation, int numElements, int sensitivityStride,
                     std::vector<std::string> counters, const std::string& fileName);
void selectCounterBM_2(SelectImplementation selectImplementation, int numElements, int sensitivityStride);
void selectCyclesCounterBM(SelectImplementation selectImplementation, int numElements,
                           int sensitivityStride, const std::string& fileName, int iterations);

#endif //MABPL_COUNTERBM_H

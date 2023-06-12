#ifndef MABPL_COUNTERBM_H
#define MABPL_COUNTERBM_H

#include "../library/select.h"

void selectCounterBM(SelectImplementation selectImplementation, int numElements, int sensitivityStride,
                     std::vector<std::string> counters, const std::string& fileName);
void selectCounterBM_2(SelectImplementation selectImplementation, int numElements, int sensitivityStride);

#endif //MABPL_COUNTERBM_H

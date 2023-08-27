#ifndef MABPL_RADIXPARTITION_H
#define MABPL_RADIXPARTITION_H

namespace MABPL {

template<typename T>
void radixPartition(int n, T *keys, int radixBits = 10);

}

#include "radixPartitionImplementation.h"

#endif //MABPL_RADIXPARTITION_H

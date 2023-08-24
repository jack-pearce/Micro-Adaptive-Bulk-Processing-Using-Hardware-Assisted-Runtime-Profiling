#ifndef MABPL_RADIXPARTITION_H
#define MABPL_RADIXPARTITION_H

namespace MABPL {

template<typename T1, typename T2>
void radixPartition(int n, T1 *keys, T2 *payloads, int radixBits = 10);

}

#include "radixPartitionImplementation.h"

#endif //MABPL_RADIXPARTITION_H

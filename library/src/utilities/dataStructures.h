#ifndef MABPL_DATASTRUCTURES_H
#define MABPL_DATASTRUCTURES_H

#include <vector>

namespace MABPL {

template<typename T1, typename T2>
using vectorOfPairs = std::vector<std::pair<T1, T2>>;

template <typename T1, typename T2>
void sortVectorOfPairs(vectorOfPairs<T1, T2> *&inputVectorOfPairs);

}

#include "dataStructuresImplementation.h"


#endif //MABPL_DATASTRUCTURES_H

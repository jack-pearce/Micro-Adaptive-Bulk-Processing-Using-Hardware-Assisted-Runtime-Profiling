#ifndef HAQP_DATASTRUCTURES_H
#define HAQP_DATASTRUCTURES_H

#include <vector>

namespace HAQP {

template<typename T1, typename T2>
using vectorOfPairs = std::vector<std::pair<T1, T2>>;

template <typename T1, typename T2>
void sortVectorOfPairs(vectorOfPairs<T1, T2> *&inputVectorOfPairs);

}

#include "dataStructuresImplementation.h"


#endif //HAQP_DATASTRUCTURES_H

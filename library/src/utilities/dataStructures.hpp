#ifndef HAQP_DATASTRUCTURES_HPP
#define HAQP_DATASTRUCTURES_HPP

#include <vector>

namespace HAQP {

template<typename T1, typename T2>
using vectorOfPairs = std::vector<std::pair<T1, T2>>;

template <typename T1, typename T2>
void sortVectorOfPairs(vectorOfPairs<T1, T2> *&inputVectorOfPairs);

}

#include "dataStructuresImplementation.hpp"


#endif //HAQP_DATASTRUCTURES_HPP

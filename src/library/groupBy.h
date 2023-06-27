#ifndef MABPL_GROUPBY_H
#define MABPL_GROUPBY_H

#include <string>
#include <vector>

enum GroupBy {
    Hash,
    Sort,
    Adaptive
};

std::vector<std::pair<int, int>> groupByHash(int n, const int *inputData);

std::vector<std::pair<int, int>> runGroupByFunction(GroupBy groupByImplementation, int n, const int *inputData);

std::string getGroupByName(GroupBy groupByImplementation);

#endif //MABPL_GROUPBY_H

#ifndef MABPL_GROUPBY_H
#define MABPL_GROUPBY_H

#include <string>
#include <vector>
#include <memory>

using Run = std::vector<std::pair<int, int>>;
using Runs = std::vector<std::unique_ptr<Run>>;

enum GroupBy {
    Hash,
    SortRadix,
    SortRadixOpt,
    SortInsertion,
    Adaptive
};

std::vector<std::pair<int, int>> groupByHash(int n, const int *inputData);

std::vector<std::pair<int, int>> groupBySortRadix(int n, int *inputData);

std::vector<std::pair<int, int>> groupBySortRadixOpt(int n, int *inputData);

std::vector<std::pair<int, int>> groupByAdaptive(int n, const int *inputData);

std::vector<std::pair<int, int>> runGroupByFunction(GroupBy groupByImplementation, int n, int *inputData);

std::string getGroupByName(GroupBy groupByImplementation);





Runs groupByAdaptiveHashLevelZero(int n, const int *inputData);

#endif //MABPL_GROUPBY_H

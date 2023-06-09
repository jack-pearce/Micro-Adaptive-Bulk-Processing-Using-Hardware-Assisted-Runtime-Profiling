#ifndef MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_SELECT_H
#define MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_SELECT_H

int selectBranch(int n, std::vector<int>& inputData, std::vector<int>& selection, int threshold);
int selectPredication(int n, std::vector<int>& inputData, std::vector<int>& selection, int threshold);

enum selectImplementation {
    Branch, Predication
};

#endif //MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_SELECT_H

#ifndef MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_SELECT_H
#define MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_SELECT_H

int selectBranch(int n, const int *inputData, int *selection, int threshold);
int selectPredication(int n, const int *inputData, int *selection, int threshold);
int selectAdaptive(int n, const int *inputData, int *selection, int threshold);

enum selectImplementation {
    Branch, Predication
};

#endif //MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_SELECT_H

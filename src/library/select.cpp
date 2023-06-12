#include "select.h"

int selectBranch(int n, const int *inputData, int *selection, int threshold) {
    int k = 0;
    for (int i = 0; i < n; ++i) {
        if (inputData[i] <= threshold) {
            selection[k++] = i;
        }
    }
    return k;
}

int selectPredication(int n, const int *inputData, int *selection, int threshold) {
    int k = 0;
    for (int i = 0; i < n; ++i) {
        selection[k] = i;
        k += (inputData[i] <= threshold);
    }
    return k;
}

typedef int (*SelectFunctionPtr)(int n, const int *inputData, int *selection, int threshold);

int selectAdaptive(int n, const int *inputData, int *selection, int threshold) {
    SelectFunctionPtr selectFunctionPtr = selectBranch;


    return 0;
}

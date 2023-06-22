#include <algorithm>
#include <iostream>
#include <immintrin.h>

#include "select.h"
#include "papi.h"
#include "utils.h"


int selectIndexesBranch(int n, const int *inputFilter, int *selection, int threshold) {
    int k = 0;
    for (int i = 0; i < n; ++i) {
        if (inputFilter[i] <= threshold) {
            selection[k++] = i;
        }
    }
    return k;
}

int selectIndexesPredication(int n, const int *inputFilter, int *selection, int threshold) {
    int k = 0;
    for (int i = 0; i < n; ++i) {
        selection[k] = i;
        k += (inputFilter[i] <= threshold);
    }
    return k;
}

inline int runSelectIndexesChunk(int (*&selectIndexesFunctionPtr)(int, const int *, int *, int),
                                 int tuplesToProcess,
                                 int &n,
                                 const int *&inputData,
                                 int *&selection,
                                 int threshold,
                                 int &k,
                                 int &consecutivePredications) {
    Counters::getInstance().readEventSet();
    int selected = selectIndexesFunctionPtr(tuplesToProcess, inputData, selection, threshold);
    Counters::getInstance().readEventSet();
    n -= tuplesToProcess;
    inputData += tuplesToProcess;
    selection += selected;
    k += selected;
    consecutivePredications += (selectIndexesFunctionPtr == selectIndexesPredication);
    return selected;
}

inline void performSelectIndexesAdaption(int (*&selectIndexesFunctionPtr)(int, const int *, int *, int),
                                         const long_long *counterValues,
                                         float lowerCrossoverSelectivity,
                                         float upperCrossoverSelectivity,
                                         float lowerBranchCrossoverBranchMisses,
                                         float m,
                                         float selectivity,
                                         int &consecutivePredications) {

    if (__builtin_expect(static_cast<float>(counterValues[0]) >
                         (((selectivity - lowerCrossoverSelectivity) * m) + lowerBranchCrossoverBranchMisses)
                         && selectIndexesFunctionPtr == selectIndexesBranch, false)) {
//        std::cout << "Switched to select predication" << std::endl;
        selectIndexesFunctionPtr = selectIndexesPredication;
    }

    if (__builtin_expect((selectivity < lowerCrossoverSelectivity
                         || selectivity > upperCrossoverSelectivity)
                         && selectIndexesFunctionPtr == selectIndexesPredication, false)) {
//        std::cout << "Switched to select branch" << std::endl;
        selectIndexesFunctionPtr = selectIndexesBranch;
        consecutivePredications = 0;
    }
}

int selectIndexesAdaptive(int n, const int *inputFilter, int *selection, int threshold) {
    int tuplesPerAdaption = 50000;
    int maxConsecutivePredications = 10;
    int tuplesInBranchBurst = 1000;

    float lowerCrossoverSelectivity = 0.03; // Could use a tuning function to identify these cross-over points
    float upperCrossoverSelectivity = 0.98; // Could use a tuning function to identify these cross-over points

    // Equations below are only valid at the extreme ends of selectivity
    // Y intercept of number of branch misses (at lower cross-over selectivity)
    float lowerBranchCrossoverBranchMisses = lowerCrossoverSelectivity * static_cast<float>(tuplesPerAdaption);
    float upperBranchCrossoverBranchMisses = (1 - upperCrossoverSelectivity) * static_cast<float>(tuplesPerAdaption);

    // Gradient of number of branch misses between lower cross-over selectivity and upper cross-over selectivity
    float m = (upperBranchCrossoverBranchMisses - lowerBranchCrossoverBranchMisses) /
            (upperCrossoverSelectivity - lowerCrossoverSelectivity);

    // Modified values for short branch burst chunks
    float lowerBranchCrossoverBranchMisses_BranchBurst = lowerCrossoverSelectivity * static_cast<float>(tuplesInBranchBurst);
    float upperBranchCrossoverBranchMisses_BranchBurst = (1 - upperCrossoverSelectivity) * static_cast<float>(tuplesInBranchBurst);
    float m_BranchBurst = (upperBranchCrossoverBranchMisses_BranchBurst - lowerBranchCrossoverBranchMisses_BranchBurst) /
              (upperCrossoverSelectivity - lowerCrossoverSelectivity);

    int k = 0;
    int consecutivePredications = 0;
    int tuplesToProcess;
    int selected;
    SelectIndexesFunctionPtr selectIndexesFunctionPtr = selectIndexesPredication;

    std::vector<std::string> counters = {"PERF_COUNT_HW_BRANCH_MISSES"};
    long_long *counterValues = Counters::getInstance().getEvents(counters);

    while (n > 0) {
        if (__builtin_expect(consecutivePredications == maxConsecutivePredications, false)) {
//            std::cout << "Running branch burst" << std::endl;
            selectIndexesFunctionPtr = selectIndexesBranch;
            consecutivePredications = 0;
            tuplesToProcess = std::min(n, tuplesInBranchBurst);
            selected = runSelectIndexesChunk(selectIndexesFunctionPtr, tuplesToProcess, n,
                                             inputFilter, selection, threshold, k, consecutivePredications);
            performSelectIndexesAdaption(selectIndexesFunctionPtr, counterValues, lowerCrossoverSelectivity,
                                         upperCrossoverSelectivity,
                                         lowerBranchCrossoverBranchMisses_BranchBurst,
                                         m_BranchBurst,
                                         static_cast<float>(selected) / static_cast<float>(tuplesInBranchBurst),
                                         consecutivePredications);
        } else {
            tuplesToProcess = std::min(n, tuplesPerAdaption);
            selected = runSelectIndexesChunk(selectIndexesFunctionPtr, tuplesToProcess, n, inputFilter, selection,
                                             threshold, k, consecutivePredications);
            performSelectIndexesAdaption(selectIndexesFunctionPtr, counterValues, lowerCrossoverSelectivity,
                                         upperCrossoverSelectivity,
                                         lowerBranchCrossoverBranchMisses, m,
                                         static_cast<float>(selected) / static_cast<float>(tuplesPerAdaption),
                                         consecutivePredications);
        }
    }

    return k;
}

int selectValuesBranch(int n, const int *inputData, const int *inputFilter, int *selection, int threshold) {
    int k = 0;
    for (int i = 0; i < n; ++i) {
        if (inputFilter[i] <= threshold) {
            selection[k++] = inputData[i];
        }
    }
    return k;
}

int selectValuesPredication(int n, const int *inputData, const int *inputFilter, int *selection, int threshold) {
    int k = 0;
    for (int i = 0; i < n; ++i) {
        selection[k] = inputData[i];
//        selection[k] = inputData[(inputFilter[i] <= threshold) * i] * (inputFilter[i] <= threshold);
        k += (inputFilter[i] <= threshold);
    }
    return k;
}

int selectValuesVectorized(int n, const int *inputData, const int *inputFilter, int *selection, int threshold) {
    int k = 0;

    // Process unaligned tuples
    int unalignedCount = 0;
    for (int i = 0; !arrayIsSimd256Aligned(inputFilter + i); ++i) {
        selection[k] = inputData[i];
        k += (inputFilter[i] <= threshold);
        ++unalignedCount;
    }

    // Vectorize the loop for aligned tuples
    int simdWidth = sizeof(__m256i) / sizeof(int);
    int simdIterations = (n - unalignedCount) / simdWidth;
    __m256i thresholdVector = _mm256_set1_epi32(threshold);

    for (int i = unalignedCount; i < unalignedCount + (simdIterations * simdWidth); i += simdWidth) {
        __m256i filterVector = _mm256_load_si256((__m256i *)(inputFilter + i));

        // Compare filterVector <= thresholdVector
        __m256i cmpResult = _mm256_cmpgt_epi32(filterVector, thresholdVector);
        __mmask8 mask = ~_mm256_movemask_epi8(cmpResult);

        for (int j = 0; j < simdWidth; ++j) {
            selection[k] = inputData[i + j];
            k += (mask >> j) & 1;
        }
    }

    // Process any remaining tuples
    for (int i = unalignedCount + simdIterations * simdWidth; i < n; ++i) {
        selection[k] = inputData[i];
        k += (inputFilter[i] <= threshold);
    }

    return k;
}

/*int selectValuesVectorized(int n, const int *inputData, const int *inputFilter, int *selection, int threshold) {
    int k = 0;

    // Process unaligned tuples
    int unalignedCount = 0;
    for (int i = 0; !arrayIsSimd256Aligned(inputFilter + i); ++i) {
        selection[k] = inputData[i];
        k += (inputFilter[i] <= threshold);
        ++unalignedCount;
    }

    // Vectorize the loop for aligned tuples
    int simdWidth = sizeof(__m256i) / sizeof(int);
    int simdIterations = (n - unalignedCount) / simdWidth;
    __m256i thresholdVector = _mm256_set1_epi32(threshold);

    for (int i = unalignedCount; i < unalignedCount + (simdIterations * simdWidth); i += simdWidth) {
        __m256i filterVector = _mm256_load_si256((__m256i *)(inputFilter + i));

        // Compare filterVector <= thresholdVector
        __mmask8 mask = ~_mm256_cmpgt_epi32_mask(filterVector, thresholdVector);

        for (int j = 0; j < simdWidth; ++j) {
            selection[k] = inputData[i + j];
            k += (mask >> j) & 1;
        }
    }

    // Process any remaining tuples
    for (int i = unalignedCount + simdIterations * simdWidth; i < n; ++i) {
        selection[k] = inputData[i];
        k += (inputFilter[i] <= threshold);
    }

    return k;
}*/

inline int runSelectValuesChunk(int (*&selectValuesFunctionPtr)(int, const int *, const int *, int *, int),
                                 int tuplesToProcess,
                                 int &n,
                                 const int *&inputData,
                                 const int *&inputFilter,
                                 int *&selection,
                                 int threshold,
                                 int &k,
                                 int &consecutivePredications) {
    Counters::getInstance().readEventSet();
    int selected = selectValuesFunctionPtr(tuplesToProcess, inputData, inputFilter, selection, threshold);
    Counters::getInstance().readEventSet();
    n -= tuplesToProcess;
    inputData += tuplesToProcess;
    inputFilter += tuplesToProcess;
    selection += selected;
    k += selected;
    consecutivePredications += (selectValuesFunctionPtr == selectValuesVectorized);
    return selected;
}

inline void performSelectValuesAdaption(int (*&selectValuesFunctionPtr)(int, const int *, const int *, int *, int),
                                         const long_long *counterValues,
                                         float crossoverSelectivity,
                                         float branchCrossoverBranchMisses,
                                         float selectivity,
                                         int &consecutiveVectorized) {

    if (__builtin_expect(static_cast<float>(counterValues[0]) > branchCrossoverBranchMisses
                         && selectValuesFunctionPtr == selectValuesBranch, false)) {
        selectValuesFunctionPtr = selectValuesVectorized;
    }

    if (__builtin_expect(selectivity < crossoverSelectivity
                         && selectValuesFunctionPtr == selectValuesVectorized, false)) {
        selectValuesFunctionPtr = selectValuesBranch;
        consecutiveVectorized = 0;
    }
}

int selectValuesAdaptive(int n, const int *inputData, const int *inputFilter, int *selection, int threshold) {
    int tuplesPerAdaption = 50000;
    int maxConsecutiveVectorized = 10;
    int tuplesInBranchBurst = 1000;

    float crossoverSelectivity = 0.003; // Could use a tuning function to identify this cross-over point

    // Equation below are only valid at the extreme ends of selectivity
    float branchCrossoverBranchMisses = crossoverSelectivity * static_cast<float>(tuplesPerAdaption);

    // Modified values for short branch burst chunks
    float branchCrossoverBranchMisses_BranchBurst = crossoverSelectivity * static_cast<float>(tuplesInBranchBurst);

    int k = 0;
    int consecutiveVectorized = 0;
    int tuplesToProcess;
    int selected;
    SelectValuesFunctionPtr selectValuesFunctionPtr = selectValuesBranch;

    std::vector<std::string> counters = {"PERF_COUNT_HW_BRANCH_MISSES"};
    long_long *counterValues = Counters::getInstance().getEvents(counters);

    while (n > 0) {

        if (__builtin_expect(consecutiveVectorized == maxConsecutiveVectorized, false)) {
            selectValuesFunctionPtr = selectValuesBranch;
            consecutiveVectorized = 0;
            tuplesToProcess = std::min(n, tuplesInBranchBurst);
            selected = runSelectValuesChunk(selectValuesFunctionPtr, tuplesToProcess, n,
                                             inputData, inputFilter, selection, threshold, k, consecutiveVectorized);
            performSelectValuesAdaption(selectValuesFunctionPtr, counterValues, crossoverSelectivity,
                                        branchCrossoverBranchMisses_BranchBurst,
                                        static_cast<float>(selected) / static_cast<float>(tuplesInBranchBurst),
                                        consecutiveVectorized);
        } else {
            tuplesToProcess = std::min(n, tuplesPerAdaption);
            selected = runSelectValuesChunk(selectValuesFunctionPtr, tuplesToProcess, n,
                                            inputData, inputFilter, selection, threshold, k, consecutiveVectorized);
            performSelectValuesAdaption(selectValuesFunctionPtr, counterValues, crossoverSelectivity,
                                         branchCrossoverBranchMisses,
                                         static_cast<float>(selected) / static_cast<float>(tuplesPerAdaption),
                                         consecutiveVectorized);
        }
    }

    return k;
}

void setSelectIndexesFuncPtr(SelectIndexesFunctionPtr &selectIndexesFunctionPtr, SelectImplementation selectImplementation) {
    switch(selectImplementation) {
        case SelectImplementation::IndexesBranch:
            selectIndexesFunctionPtr = selectIndexesBranch;
            break;
        case SelectImplementation::IndexesPredication:
            selectIndexesFunctionPtr = selectIndexesPredication;
            break;
        case SelectImplementation::IndexesAdaptive:
            selectIndexesFunctionPtr = selectIndexesAdaptive;
            break;
        default:
            std::cout << "Invalid selection of 'Select' implementation!" << std::endl;
            exit(1);
    }
}

void setSelectValuesFuncPtr(SelectValuesFunctionPtr &selectValueFunctionPtr, SelectImplementation selectImplementation) {
    switch(selectImplementation) {
        case SelectImplementation::ValuesBranch:
            selectValueFunctionPtr = selectValuesBranch;
            break;
        case SelectImplementation::ValuesPredication:
            selectValueFunctionPtr = selectValuesPredication;
            break;
        case SelectImplementation::ValuesVectorized:
            selectValueFunctionPtr = selectValuesVectorized;
            break;
        case SelectImplementation::ValuesAdaptive:
            selectValueFunctionPtr = selectValuesAdaptive;
            break;
        default:
            std::cout << "Invalid selection of 'Select' implementation!" << std::endl;
            exit(1);
    }
}

int runSelectFunction(SelectImplementation selectImplementation,
                      int n, const int *inputData, const int *inputFilter, int *selection, int threshold) {
    if (selectImplementation == SelectImplementation::IndexesBranch ||
        selectImplementation == SelectImplementation::IndexesPredication ||
        selectImplementation == SelectImplementation::IndexesAdaptive) {
        SelectIndexesFunctionPtr selectIndexesFunctionPtr;
        setSelectIndexesFuncPtr(selectIndexesFunctionPtr, selectImplementation);
        return selectIndexesFunctionPtr(n, inputData, selection, threshold);
    } else {
        SelectValuesFunctionPtr selectValuesFunctionPtr;
        setSelectValuesFuncPtr(selectValuesFunctionPtr, selectImplementation);
        return selectValuesFunctionPtr(n, inputData, inputFilter, selection, threshold);
    }
}

std::string getSelectName(SelectImplementation selectImplementation) {
    switch(selectImplementation) {
        case SelectImplementation::IndexesBranch:
            return "Select_Indexes_Branch";
        case SelectImplementation::IndexesPredication:
            return "Select_Indexes_Predication";
        case SelectImplementation::IndexesAdaptive:
            return "Select_Indexes_Adaptive";
        case SelectImplementation::ValuesBranch:
            return "Select_Values_Branch";
        case SelectImplementation::ValuesPredication:
            return "Select_Values_Predication";
        case SelectImplementation::ValuesVectorized:
            return "Select_Values_Vectorized";
        case SelectImplementation::ValuesAdaptive:
            return "Select_Values_Adaptive";
        default:
            std::cout << "Invalid selection of 'Select' implementation!" << std::endl;
            exit(1);
    }
}
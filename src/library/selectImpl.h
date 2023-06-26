#ifndef MABPL_SELECT_IMPLEMENTATION_H
#define MABPL_SELECT_IMPLEMENTATION_H

#include <immintrin.h>

#include "papi.h"
#include "utils.h"


template<typename T>
int selectIndexesBranch(int n, const T *inputFilter, int *selection, T threshold) {
    auto k = 0;
    for (auto i = 0; i < n; ++i) {
        if (inputFilter[i] <= threshold) {
            selection[k++] = i;
        }
    }
    return k;
}

template<typename T>
int selectIndexesPredication(int n, const T *inputFilter, int *selection, T threshold) {
    auto k = 0;
    for (auto i = 0; i < n; ++i) {
        selection[k] = i;
        k += (inputFilter[i] <= threshold);
    }
    return k;
}

template<typename T>
inline int runSelectIndexesChunk(SelectIndexesFunctionPtr<T> selectIndexesFunctionPtr,
                                 int tuplesToProcess,
                                 int &n,
                                 const T *&inputData,
                                 int *&selection,
                                 T threshold,
                                 int &k,
                                 int &consecutivePredications) {
    Counters::getInstance().readEventSet();
    int selected = selectIndexesFunctionPtr(tuplesToProcess, inputData, selection, threshold);
    Counters::getInstance().readEventSet();
    n -= tuplesToProcess;
    inputData += tuplesToProcess;
    selection += selected;
    k += selected;
    consecutivePredications += (selectIndexesFunctionPtr == selectIndexesPredication<T>);
    return selected;
}

template<typename T>
inline void performSelectIndexesAdaption(SelectIndexesFunctionPtr<T> &selectIndexesFunctionPtr,
                                         const long_long *counterValues,
                                         float lowerCrossoverSelectivity,
                                         float upperCrossoverSelectivity,
                                         float lowerBranchCrossoverBranchMisses,
                                         float m,
                                         float selectivity,
                                         int &consecutivePredications) {

    if (__builtin_expect(static_cast<float>(counterValues[0]) >
                         (((selectivity - lowerCrossoverSelectivity) * m) + lowerBranchCrossoverBranchMisses)
                         && selectIndexesFunctionPtr == selectIndexesBranch<T>, false)) {
//        std::cout << "Switched to select predication" << std::endl;
        selectIndexesFunctionPtr = selectIndexesPredication;
    }

    if (__builtin_expect((selectivity < lowerCrossoverSelectivity
                          || selectivity > upperCrossoverSelectivity)
                         && selectIndexesFunctionPtr == selectIndexesPredication<T>, false)) {
//        std::cout << "Switched to select branch" << std::endl;
        selectIndexesFunctionPtr = selectIndexesBranch;
        consecutivePredications = 0;
    }
}

template<typename T>
int selectIndexesAdaptive(int n, const T *inputFilter, int *selection, T threshold) {
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
    SelectIndexesFunctionPtr<T> selectIndexesFunctionPtr = selectIndexesPredication<T>;

    std::vector<std::string> counters = {"PERF_COUNT_HW_BRANCH_MISSES"};
    long_long *counterValues = Counters::getInstance().getEvents(counters);

    while (n > 0) {
        if (__builtin_expect(consecutivePredications == maxConsecutivePredications, false)) {
//            std::cout << "Running branch burst" << std::endl;
            selectIndexesFunctionPtr = selectIndexesBranch;
            consecutivePredications = 0;
            tuplesToProcess = std::min(n, tuplesInBranchBurst);
            selected = runSelectIndexesChunk<T>(selectIndexesFunctionPtr, tuplesToProcess, n,
                                                inputFilter, selection, threshold, k, consecutivePredications);
            performSelectIndexesAdaption(selectIndexesFunctionPtr, counterValues, lowerCrossoverSelectivity,
                                         upperCrossoverSelectivity,
                                         lowerBranchCrossoverBranchMisses_BranchBurst,
                                         m_BranchBurst,
                                         static_cast<float>(selected) / static_cast<float>(tuplesInBranchBurst),
                                         consecutivePredications);
        } else {
            tuplesToProcess = std::min(n, tuplesPerAdaption);
            selected = runSelectIndexesChunk<T>(selectIndexesFunctionPtr, tuplesToProcess, n, inputFilter, selection,
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

template<typename T1, typename T2>
int selectValuesBranch(int n, const T2 *inputData, const T1 *inputFilter, T2 *selection, T1 threshold) {
    auto k = 0;
    for (auto i = 0; i < n; ++i) {
        if (inputFilter[i] <= threshold) {
            selection[k++] = inputData[i];
        }
    }
    return k;
}

template<typename T1, typename T2>
int selectValuesPredication(int n, const T2 *inputData, const T1 *inputFilter, T2 *selection, T1 threshold) {
    auto k = 0;
    for (auto i = 0; i < n; ++i) {
        selection[k] = inputData[i];
//        selection[k] = inputData[(inputFilter[i] <= threshold) * i] * (inputFilter[i] <= threshold);
        k += (inputFilter[i] <= threshold);
    }
    return k;
}

template<typename T1, typename T2>
int selectValuesVectorized128(int n, const T2 *inputData, const T1 *inputFilter, T2 *selection, T1 threshold) {
    auto k = 0;

    // Process unaligned tuples
    auto unalignedCount = 0;
    for (auto i = 0; !arrayIsSimd128Aligned(inputFilter + i); ++i) {
        selection[k] = inputData[i];
        k += (inputFilter[i] <= threshold);
        ++unalignedCount;
    }

    // Vectorize the loop for aligned tuples
    int simdWidth = sizeof(__m128i) / sizeof(int);
    int simdIterations = (n - unalignedCount) / simdWidth;
    __m128i thresholdVector = _mm_set1_epi32(threshold);

    for (auto i = unalignedCount; i < unalignedCount + (simdIterations * simdWidth); i += simdWidth) {
        __m128i filterVector = _mm_load_si128((__m128i *)(inputFilter + i));

        // Compare filterVector <= thresholdVector
        __m128i cmpResult = _mm_cmpgt_epi32(filterVector, thresholdVector);
        int mask = ~_mm_movemask_epi8(cmpResult);

        for (auto j = 0; j < simdWidth; ++j) {
            selection[k] = inputData[i + j];
            k += (mask >> (j * 4)) & 1;
        }
    }

    // Process any remaining tuples
    for (auto i = unalignedCount + simdIterations * simdWidth; i < n; ++i) {
        selection[k] = inputData[i];
        k += (inputFilter[i] <= threshold);
    }

    return k;
}

template<typename T1, typename T2>
int selectValuesVectorized256(int n, const T2 *inputData, const T1 *inputFilter, T2 *selection, T1 threshold) {
    auto k = 0;

    // Process unaligned tuples
    auto unalignedCount = 0;
    for (auto i = 0; !arrayIsSimd256Aligned(inputFilter + i); ++i) {
        selection[k] = inputData[i];
        k += (inputFilter[i] <= threshold);
        ++unalignedCount;
    }

    // Vectorize the loop for aligned tuples
    int simdWidth = sizeof(__m256i) / sizeof(int);
    int simdIterations = (n - unalignedCount) / simdWidth;
    __m256i thresholdVector = _mm256_set1_epi32(threshold);

    for (auto i = unalignedCount; i < unalignedCount + (simdIterations * simdWidth); i += simdWidth) {
        __m256i filterVector = _mm256_load_si256((__m256i *)(inputFilter + i));

        // Compare filterVector <= thresholdVector
        __mmask8 mask = ~_mm256_cmpgt_epi32_mask(filterVector, thresholdVector);

        for (auto j = 0; j < simdWidth; ++j) {
            selection[k] = inputData[i + j];
            k += (mask >> j) & 1;
        }
    }

    // Process any remaining tuples
    for (auto i = unalignedCount + simdIterations * simdWidth; i < n; ++i) {
        selection[k] = inputData[i];
        k += (inputFilter[i] <= threshold);
    }

    return k;
}

template<typename T1, typename T2>
int selectValuesVectorized(int n, const T2 *inputData, const T1 *inputFilter, T2 *selection, T1 threshold) {
    if (__builtin_cpu_supports("avx2")) {
        std::cout << "Machine does support avx2" << std::endl;
        return selectValuesVectorized256(n, inputData, inputFilter, selection ,threshold);
    } else {
        std::cout << "Machine does not support avx2" << std::endl;
        return selectValuesVectorized128(n, inputData, inputFilter, selection ,threshold);
    }
}

template<typename T1, typename T2>
inline int runSelectValuesChunk(SelectValuesFunctionPtr<T1,T2> selectValuesFunctionPtr,
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
    consecutivePredications += (selectValuesFunctionPtr == selectValuesVectorized<T1,T2>);
    return selected;
}

template<typename T1, typename T2>
inline void performSelectValuesAdaption(SelectValuesFunctionPtr<T1,T2> &selectValuesFunctionPtr,
                                        const long_long *counterValues,
                                        float crossoverSelectivity,
                                        float branchCrossoverBranchMisses,
                                        float selectivity,
                                        int &consecutiveVectorized) {

    if (__builtin_expect(static_cast<float>(counterValues[0]) > branchCrossoverBranchMisses
                         && selectValuesFunctionPtr == selectValuesBranch<T1,T2>, false)) {
        selectValuesFunctionPtr = selectValuesVectorized;
    }

    if (__builtin_expect(selectivity < crossoverSelectivity
                         && selectValuesFunctionPtr == selectValuesVectorized<T1,T2>, false)) {
        selectValuesFunctionPtr = selectValuesBranch;
        consecutiveVectorized = 0;
    }
}

template<typename T1, typename T2>
int selectValuesAdaptive(int n, const T2 *inputData, const T1 *inputFilter, T2 *selection, T1 threshold) {
    auto tuplesPerAdaption = 50000;
    auto maxConsecutiveVectorized = 10;
    auto tuplesInBranchBurst = 1000;

    float crossoverSelectivity = 0.003; // Could use a tuning function to identify this cross-over point

    // Equation below are only valid at the extreme ends of selectivity
    float branchCrossoverBranchMisses = crossoverSelectivity * static_cast<float>(tuplesPerAdaption);

    // Modified values for short branch burst chunks
    float branchCrossoverBranchMisses_BranchBurst = crossoverSelectivity * static_cast<float>(tuplesInBranchBurst);

    auto k = 0;
    auto consecutiveVectorized = 0;
    int tuplesToProcess;
    int selected;
    SelectValuesFunctionPtr<T1,T2> selectValuesFunctionPtr = selectValuesBranch<T1,T2>;

    std::vector<std::string> counters = {"PERF_COUNT_HW_BRANCH_MISSES"};
    long_long *counterValues = Counters::getInstance().getEvents(counters);

    while (n > 0) {

        if (__builtin_expect(consecutiveVectorized == maxConsecutiveVectorized, false)) {
            selectValuesFunctionPtr = selectValuesBranch;
            consecutiveVectorized = 0;
            tuplesToProcess = std::min(n, tuplesInBranchBurst);
            selected = runSelectValuesChunk<T1,T2>(selectValuesFunctionPtr, tuplesToProcess, n,
                                            inputData, inputFilter, selection, threshold, k, consecutiveVectorized);
            performSelectValuesAdaption(selectValuesFunctionPtr, counterValues, crossoverSelectivity,
                                        branchCrossoverBranchMisses_BranchBurst,
                                        static_cast<float>(selected) / static_cast<float>(tuplesInBranchBurst),
                                        consecutiveVectorized);
        } else {
            tuplesToProcess = std::min(n, tuplesPerAdaption);
            selected = runSelectValuesChunk<T1,T2>(selectValuesFunctionPtr, tuplesToProcess, n,
                                            inputData, inputFilter, selection, threshold, k, consecutiveVectorized);
            performSelectValuesAdaption(selectValuesFunctionPtr, counterValues, crossoverSelectivity,
                                        branchCrossoverBranchMisses,
                                        static_cast<float>(selected) / static_cast<float>(tuplesPerAdaption),
                                        consecutiveVectorized);
        }
    }

    return k;
}


template<typename T1, typename T2>
int runSelectFunction(SelectImplementation selectImplementation,
                      int n, const T2 *inputData, const T1 *inputFilter, T2 *selection, T1 threshold) {
    switch(selectImplementation) {
        case SelectImplementation::IndexesBranch:
            return selectIndexesBranch(n, inputFilter, selection, threshold);
        case SelectImplementation::IndexesPredication:
            return selectIndexesPredication(n, inputFilter, selection, threshold);
        case SelectImplementation::IndexesAdaptive:
            return selectIndexesAdaptive(n, inputFilter, selection, threshold);
        case SelectImplementation::ValuesBranch:
            return selectValuesBranch(n, inputData, inputFilter, selection, threshold);
        case SelectImplementation::ValuesPredication:
            return selectValuesPredication(n, inputData, inputFilter, selection, threshold);
        case SelectImplementation::ValuesVectorized:
            return selectValuesVectorized(n, inputData, inputFilter, selection, threshold);
        case SelectImplementation::ValuesAdaptive:
            return selectValuesAdaptive(n, inputData, inputFilter, selection, threshold);
        default:
            std::cout << "Invalid selection of 'Select' implementation!" << std::endl;
            exit(1);
    }
}

#endif //MABPL_SELECT_IMPLEMENTATION_H

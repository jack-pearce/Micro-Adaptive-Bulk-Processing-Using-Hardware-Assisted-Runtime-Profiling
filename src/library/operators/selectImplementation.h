#ifndef MABPL_SELECT_IMPLEMENTATION_H
#define MABPL_SELECT_IMPLEMENTATION_H

#include <immintrin.h>
#include <functional>

#include "../utilities/papi.h"
#include "../utilities/systemInformation.h"


namespace MABPL {

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
inline int runSelectIndexesChunk(SelectIndexesChoice selectIndexesChoice,
                                 int tuplesToProcess,
                                 int &n,
                                 const T *&inputData,
                                 int *&selection,
                                 T threshold,
                                 int &k,
                                 int &consecutivePredications) {
    int selected;
    if (selectIndexesChoice == SelectIndexesChoice::IndexesBranch) {
        Counters::getInstance().readSharedEventSet();
        selected = selectIndexesBranch(tuplesToProcess, inputData, selection, threshold);
        Counters::getInstance().readSharedEventSet();
    } else {
        Counters::getInstance().readSharedEventSet();
        selected = selectIndexesPredication(tuplesToProcess, inputData, selection, threshold);
        Counters::getInstance().readSharedEventSet();
    }
    n -= tuplesToProcess;
    inputData += tuplesToProcess;
    selection += selected;
    k += selected;
    consecutivePredications += (selectIndexesChoice == SelectIndexesChoice::IndexesPredication);
    return selected;
}

inline void performSelectIndexesAdaption(SelectIndexesChoice &selectIndexesChoice,
                                         const long_long *counterValues,
                                         float lowerCrossoverSelectivity,
                                         float upperCrossoverSelectivity,
                                         float lowerBranchCrossoverBranchMisses,
                                         float m,
                                         float selectivity,
                                         int &consecutivePredications) {

    if (__builtin_expect(static_cast<float>(counterValues[0]) >
                         (((selectivity - lowerCrossoverSelectivity) * m) + lowerBranchCrossoverBranchMisses)
                         && selectIndexesChoice == SelectIndexesChoice::IndexesBranch, false)) {
//        std::cout << "Switched to select predication" << std::endl;
        selectIndexesChoice = SelectIndexesChoice::IndexesPredication;
    }

    if (__builtin_expect((selectivity < lowerCrossoverSelectivity
                          || selectivity > upperCrossoverSelectivity)
                         && selectIndexesChoice == SelectIndexesChoice::IndexesPredication, false)) {
//        std::cout << "Switched to select branch" << std::endl;
        selectIndexesChoice = SelectIndexesChoice::IndexesBranch;
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
    SelectIndexesChoice selectIndexesChoice = SelectIndexesChoice::IndexesPredication;

    std::vector<std::string> counters = {"PERF_COUNT_HW_BRANCH_MISSES"};
    long_long *counterValues = Counters::getInstance().getSharedEventSetEvents(counters);

    while (n > 0) {
        if (__builtin_expect(consecutivePredications == maxConsecutivePredications, false)) {
//            std::cout << "Running branch burst" << std::endl;
            selectIndexesChoice = SelectIndexesChoice::IndexesBranch;
            consecutivePredications = 0;
            tuplesToProcess = std::min(n, tuplesInBranchBurst);
            selected = runSelectIndexesChunk<T>(selectIndexesChoice, tuplesToProcess, n,
                                                inputFilter, selection, threshold, k, consecutivePredications);
            performSelectIndexesAdaption(selectIndexesChoice, counterValues, lowerCrossoverSelectivity,
                                         upperCrossoverSelectivity,
                                         lowerBranchCrossoverBranchMisses_BranchBurst,
                                         m_BranchBurst,
                                         static_cast<float>(selected) / static_cast<float>(tuplesInBranchBurst),
                                         consecutivePredications);
        } else {
            tuplesToProcess = std::min(n, tuplesPerAdaption);
            selected = runSelectIndexesChunk<T>(selectIndexesChoice, tuplesToProcess, n, inputFilter, selection,
                                                threshold, k, consecutivePredications);
            performSelectIndexesAdaption(selectIndexesChoice, counterValues, lowerCrossoverSelectivity,
                                         upperCrossoverSelectivity,
                                         lowerBranchCrossoverBranchMisses, m,
                                         static_cast<float>(selected) / static_cast<float>(tuplesPerAdaption),
                                         consecutivePredications);
        }
    }

    return k;
}

template<typename T>
int selectIndexesAdaptiveParallel(int n, const T *inputFilter, int *selection, T threshold, int dop) {

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
//        selection[k] = inputData[(0L - (inputFilter[i] <= threshold)) & i];
        k += (inputFilter[i] <= threshold);
    }
    return k;
}


#ifdef __AVX2__

template<typename T1, typename T2>
int selectValuesVectorized(int n, const T2 *inputData, const T1 *inputFilter, T2 *selection, T1 threshold) {
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

#else

template<typename T1, typename T2>
int selectValuesVectorized(int n, const T2 *inputData, const T1 *inputFilter, T2 *selection, T1 threshold) {
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

#endif

template<typename T1, typename T2>
inline int runSelectValuesChunk(SelectValuesChoice selectValuesChoice,
                                int tuplesToProcess,
                                int &n,
                                const T2 *&inputData,
                                const T1 *&inputFilter,
                                T2 *&selection,
                                T1 threshold,
                                int &k,
                                int &consecutivePredications) {
    int selected;
    if (selectValuesChoice == SelectValuesChoice::ValuesBranch) {
        Counters::getInstance().readSharedEventSet();
        selected = selectValuesBranch(tuplesToProcess, inputData, inputFilter, selection, threshold);
        Counters::getInstance().readSharedEventSet();
    } else {
        Counters::getInstance().readSharedEventSet();
        selected = selectValuesVectorized(tuplesToProcess, inputData, inputFilter, selection, threshold);
        Counters::getInstance().readSharedEventSet();
    }
    n -= tuplesToProcess;
    inputData += tuplesToProcess;
    inputFilter += tuplesToProcess;
    selection += selected;
    k += selected;
    consecutivePredications += (selectValuesChoice == SelectValuesChoice::ValuesVectorized);
    return selected;
}

inline void performSelectValuesAdaption(SelectValuesChoice &selectValuesChoice,
                                        const long_long *counterValues,
                                        float crossoverSelectivity,
                                        float branchCrossoverBranchMisses,
                                        float selectivity,
                                        int &consecutiveVectorized) {

    if (__builtin_expect(static_cast<float>(counterValues[0]) > branchCrossoverBranchMisses
                         && selectValuesChoice == SelectValuesChoice::ValuesBranch, false)) {
        selectValuesChoice = SelectValuesChoice::ValuesVectorized;
    }

    if (__builtin_expect(selectivity < crossoverSelectivity
                         && selectValuesChoice == SelectValuesChoice::ValuesVectorized, false)) {
        selectValuesChoice = SelectValuesChoice::ValuesBranch;
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
    SelectValuesChoice selectValuesChoice = SelectValuesChoice::ValuesBranch;

    std::vector<std::string> counters = {"PERF_COUNT_HW_BRANCH_MISSES"};
    long_long *counterValues = Counters::getInstance().getSharedEventSetEvents(counters);

    while (n > 0) {

        if (__builtin_expect(consecutiveVectorized == maxConsecutiveVectorized, false)) {
            selectValuesChoice = SelectValuesChoice::ValuesVectorized;
            consecutiveVectorized = 0;
            tuplesToProcess = std::min(n, tuplesInBranchBurst);
            selected = runSelectValuesChunk<T1,T2>(selectValuesChoice, tuplesToProcess, n,
                                            inputData, inputFilter, selection, threshold, k, consecutiveVectorized);
            performSelectValuesAdaption(selectValuesChoice, counterValues, crossoverSelectivity,
                                        branchCrossoverBranchMisses_BranchBurst,
                                        static_cast<float>(selected) / static_cast<float>(tuplesInBranchBurst),
                                        consecutiveVectorized);
        } else {
            tuplesToProcess = std::min(n, tuplesPerAdaption);
            selected = runSelectValuesChunk<T1,T2>(selectValuesChoice, tuplesToProcess, n,
                                            inputData, inputFilter, selection, threshold, k, consecutiveVectorized);
            performSelectValuesAdaption(selectValuesChoice, counterValues, crossoverSelectivity,
                                        branchCrossoverBranchMisses,
                                        static_cast<float>(selected) / static_cast<float>(tuplesPerAdaption),
                                        consecutiveVectorized);
        }
    }

    return k;
}

template<typename T1, typename T2>
int selectValuesAdaptive(int n, const T2 *inputData, const T1 *inputFilter, T2 *selection, T1 threshold, int dop) {

}

template<typename T1, typename T2>
int runSelectFunction(Select selectImplementation,
                      int n, const T2 *inputData, const T1 *inputFilter, T2 *selection, T1 threshold, int dop) {
    switch(selectImplementation) {
        case Select::ImplementationIndexesBranch:
            static_assert(std::is_same<T2, int>::value, "selection array type must be int for select indexes function");
            return selectIndexesBranch(n, inputFilter, selection, threshold);
        case Select::ImplementationIndexesPredication:
            static_assert(std::is_same<T2, int>::value, "selection array type must be int for select indexes function");
            return selectIndexesPredication(n, inputFilter, selection, threshold);
        case Select::ImplementationIndexesAdaptive:
            static_assert(std::is_same<T2, int>::value, "selection array type must be int for select indexes function");
            return selectIndexesAdaptive(n, inputFilter, selection, threshold);
        case Select::ImplementationIndexesAdaptiveParallel:
            static_assert(std::is_same<T2, int>::value, "selection array type must be int for select indexes function");
            return selectIndexesAdaptiveParallel(n, inputFilter, selection, threshold, dop);
        case Select::ImplementationValuesBranch:
            return selectValuesBranch(n, inputData, inputFilter, selection, threshold);
        case Select::ImplementationValuesPredication:
            return selectValuesPredication(n, inputData, inputFilter, selection, threshold);
        case Select::ImplementationValuesVectorized:
            return selectValuesVectorized(n, inputData, inputFilter, selection, threshold);
        case Select::ImplementationValuesAdaptive:
            return selectValuesAdaptive(n, inputData, inputFilter, selection, threshold);
        case Select::ImplementationValuesAdaptiveParallel:
            return selectValuesAdaptive(n, inputData, inputFilter, selection, threshold, dop);
        default:
            std::cout << "Invalid selection of 'Select' implementation!" << std::endl;
            exit(1);
    }
}

}

#endif //MABPL_SELECT_IMPLEMENTATION_H

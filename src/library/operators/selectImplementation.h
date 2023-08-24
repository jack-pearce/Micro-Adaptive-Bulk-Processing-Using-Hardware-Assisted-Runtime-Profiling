#ifndef MABPL_SELECT_IMPLEMENTATION_H
#define MABPL_SELECT_IMPLEMENTATION_H

#include <immintrin.h>
#include <functional>
#include <cassert>
#include <atomic>
#include <cstring>

#include "../utilities/papi.h"
#include "../utilities/systemInformation.h"
#include "../utilities/dataStructures.h"
#include "../machine_constants/machineConstants.h"


namespace MABPL {

template<typename T>
int selectIndexesBranch(int endIndex, const T *inputFilter, int *selection, T threshold, int startIndex) {
    auto k = 0;
    for (auto i = startIndex; i < endIndex; ++i) {
        if (inputFilter[i] <= threshold) {
            selection[k++] = i;
        }
    }
    return k;
}

template<typename T>
int selectIndexesPredication(int endIndex, const T *inputFilter, int *selection, T threshold, int startIndex) {
    auto k = 0;
    for (auto i = startIndex; i < endIndex; ++i) {
        selection[k] = i;
        k += (inputFilter[i] <= threshold);
    }
    return k;
}

template<typename T>
inline int runSelectIndexesChunk(SelectIndexesChoice selectIndexesChoice,
                                 int tuplesToProcess,
                                 int &startIndex,
                                 int &n,
                                 const T *inputFilter,
                                 int *&selection,
                                 T threshold,
                                 int &k,
                                 int &consecutivePredications) {
    int selected;
    if (selectIndexesChoice == SelectIndexesChoice::IndexesBranch) {
        Counters::getInstance().readSharedEventSet();
        selected = selectIndexesBranch(startIndex + tuplesToProcess, inputFilter, selection, threshold,
                                       startIndex);
        Counters::getInstance().readSharedEventSet();
    } else {
        Counters::getInstance().readSharedEventSet();
        selected = selectIndexesPredication(startIndex + tuplesToProcess, inputFilter, selection,
                                            threshold, startIndex);
        Counters::getInstance().readSharedEventSet();
    }
    n -= tuplesToProcess;
    startIndex += tuplesToProcess;
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
        std::cout << "Switched to select predication" << std::endl;
        selectIndexesChoice = SelectIndexesChoice::IndexesPredication;
    }

    if (__builtin_expect((selectivity < lowerCrossoverSelectivity
                          || selectivity > upperCrossoverSelectivity)
                         && selectIndexesChoice == SelectIndexesChoice::IndexesPredication, false)) {
        std::cout << "Switched to select branch" << std::endl;
        selectIndexesChoice = SelectIndexesChoice::IndexesBranch;
        consecutivePredications = 0;
    }
}

template<typename T>
int selectIndexesAdaptive(int n, const T *inputFilter, int *selection, T threshold) {
    int tuplesPerAdaption = 50000;
    int maxConsecutivePredications = 10;
    int tuplesInBranchBurst = 1000;

    std::string lowerMachineConstantName = "SelectIndexesLower_" + std::to_string(sizeof(T)) + "B_inputFilter_1_dop";
    std::string upperMachineConstantName = "SelectIndexesUpper_" + std::to_string(sizeof(T)) + "B_inputFilter_1_dop";
    float lowerCrossoverSelectivity = MachineConstants::getInstance().getMachineConstant(lowerMachineConstantName);
    float upperCrossoverSelectivity = MachineConstants::getInstance().getMachineConstant(upperMachineConstantName);

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

    int startIndex = 0;
    int k = 0;
    int consecutivePredications = 0;
    int tuplesToProcess;
    int selected;
    SelectIndexesChoice selectIndexesChoice = SelectIndexesChoice::IndexesPredication;

    std::vector<std::string> counters = {"PERF_COUNT_HW_BRANCH_MISSES"};
    long_long *counterValues = Counters::getInstance().getSharedEventSetEvents(counters);

    while (n > 0) {
        if (__builtin_expect(consecutivePredications == maxConsecutivePredications, false)) {
            std::cout << "Running branch burst" << std::endl;
            selectIndexesChoice = SelectIndexesChoice::IndexesBranch;
            consecutivePredications = 0;
            tuplesToProcess = std::min(n, tuplesInBranchBurst);
            selected = runSelectIndexesChunk<T>(selectIndexesChoice, tuplesToProcess, startIndex,n,
                                                inputFilter, selection, threshold, k,
                                                consecutivePredications);
            performSelectIndexesAdaption(selectIndexesChoice, counterValues, lowerCrossoverSelectivity,
                                         upperCrossoverSelectivity,
                                         lowerBranchCrossoverBranchMisses_BranchBurst,
                                         m_BranchBurst,
                                         static_cast<float>(selected) / static_cast<float>(tuplesInBranchBurst),
                                         consecutivePredications);
        } else {
            tuplesToProcess = std::min(n, tuplesPerAdaption);
            selected = runSelectIndexesChunk<T>(selectIndexesChoice, tuplesToProcess, startIndex,n,
                                                inputFilter, selection, threshold, k,
                                                consecutivePredications);
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
struct SelectIndexesThreadArgs {
    vectorOfPairs<int, int> *chunkPositions;
    std::atomic<int> *chunksCompleted;
    int maxSize;
    const T* inputFilter;
    int *selection;
    T threshold;
    std::atomic<int> *selectedCount;
    int dop;
};

template<typename T>
inline int runSelectIndexesChunkParallel(SelectIndexesChoice selectIndexesChoice,
                                         int tuplesToProcess,
                                         int &startIndex,
                                         int &n,
                                         const T *inputFilter,
                                         int *&selection,
                                         T threshold,
                                         int &k,
                                         int &consecutivePredications,
                                         int eventSet,
                                         long_long *counterValues) {
    int selected;
    if (selectIndexesChoice == SelectIndexesChoice::IndexesBranch) {
        readThreadEventSet(eventSet, 1, counterValues);
        selected = selectIndexesBranch(startIndex + tuplesToProcess, inputFilter, selection, threshold,
                                       startIndex);
        readThreadEventSet(eventSet, 1, counterValues);
    } else {
        readThreadEventSet(eventSet, 1, counterValues);
        selected = selectIndexesPredication(startIndex + tuplesToProcess, inputFilter, selection,
                                            threshold, startIndex);
        readThreadEventSet(eventSet, 1, counterValues);
    }
    n -= tuplesToProcess;
    startIndex += tuplesToProcess;
    selection += selected;
    k += selected;
    consecutivePredications += (selectIndexesChoice == SelectIndexesChoice::IndexesPredication);
    return selected;
}

template<typename T>
void *selectIndexesAdaptiveParallelAux(void *arg) {
    auto* args = static_cast<SelectIndexesThreadArgs<T>*>(arg);
    vectorOfPairs<int, int> *chunkPositions = args->chunkPositions;
    std::atomic<int> *chunksCompleted = args->chunksCompleted;
    int maxSize = args->maxSize;
    const T *inputFilter = args->inputFilter;
    int *overallSelection = args->selection;
    T threshold = args->threshold;
    std::atomic<int> *selectedCount = args->selectedCount;
    int dop = args->dop;

    auto threadSelection = new int[maxSize];
    auto threadSelectionStart = threadSelection;

    int tuplesPerAdaption = 50000;
    int maxConsecutivePredications = 10;
    int tuplesInBranchBurst = 1000;

    std::string lowerMachineConstantName = "SelectIndexesLower_" + std::to_string(sizeof(T)) + "B_inputFilter_" +
            std::to_string(dop) + "_dop";
    std::string upperMachineConstantName = "SelectIndexesUpper_" + std::to_string(sizeof(T)) + "B_inputFilter_" +
            std::to_string(dop) + "_dop";
    float lowerCrossoverSelectivity = MachineConstants::getInstance().getMachineConstant(lowerMachineConstantName);
    float upperCrossoverSelectivity = MachineConstants::getInstance().getMachineConstant(upperMachineConstantName);

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

    int eventSet = PAPI_NULL;
    std::vector<std::string> counters = {"PERF_COUNT_HW_BRANCH_MISSES"};
    long_long counterValues[1] = {0};
    createThreadEventSet(&eventSet, counters);

    int nextChunk = (*chunksCompleted).fetch_add(1);
    int kTotal = 0;
    int startIndex, n, k, consecutivePredications, tuplesToProcess, selected;

    while (nextChunk < static_cast<int>(chunkPositions->size())) {

        startIndex = (*chunkPositions)[nextChunk].first;
        n = (*chunkPositions)[nextChunk].second;
        k = 0;
        consecutivePredications = 0;
        SelectIndexesChoice selectIndexesChoice = SelectIndexesChoice::IndexesPredication;


        while (n > 0) {
            if (__builtin_expect(consecutivePredications == maxConsecutivePredications, false)) {
                selectIndexesChoice = SelectIndexesChoice::IndexesBranch;
                consecutivePredications = 0;
                tuplesToProcess = std::min(n, tuplesInBranchBurst);
                selected = runSelectIndexesChunkParallel<T>(selectIndexesChoice, tuplesToProcess, startIndex, n,
                                                            inputFilter, threadSelection, threshold, k,
                                                            consecutivePredications, eventSet, counterValues);
                performSelectIndexesAdaption(selectIndexesChoice, counterValues, lowerCrossoverSelectivity,
                                             upperCrossoverSelectivity,
                                             lowerBranchCrossoverBranchMisses_BranchBurst,
                                             m_BranchBurst,
                                             static_cast<float>(selected) / static_cast<float>(tuplesInBranchBurst),
                                             consecutivePredications);
            } else {
                tuplesToProcess = std::min(n, tuplesPerAdaption);
                selected = runSelectIndexesChunkParallel<T>(selectIndexesChoice, tuplesToProcess, startIndex, n,
                                                            inputFilter, threadSelection, threshold, k,
                                                            consecutivePredications, eventSet, counterValues);
                performSelectIndexesAdaption(selectIndexesChoice, counterValues, lowerCrossoverSelectivity,
                                             upperCrossoverSelectivity,
                                             lowerBranchCrossoverBranchMisses, m,
                                             static_cast<float>(selected) / static_cast<float>(tuplesPerAdaption),
                                             consecutivePredications);
            }
        }

        kTotal += k;
        nextChunk = (*chunksCompleted).fetch_add(1);
    }

    destroyThreadEventSet(eventSet, counterValues);

    int overallSelectionStartIndex = (*selectedCount).fetch_add(kTotal);
    memcpy(overallSelection + overallSelectionStartIndex, threadSelectionStart, kTotal * sizeof(int));

    delete []threadSelectionStart;
    delete args;

    return nullptr;
}

template<typename T>
int selectIndexesAdaptiveParallel(int n, const T *inputFilter, int *selection, T threshold, int dop) {
    assert(1 < dop && dop <= maxDop());

    Counters::getInstance();
    MachineConstants::getInstance();
    pthread_t threads[dop];

    int adaptivePeriod = 50000;
    int tuplesPerChunk = std::max(adaptivePeriod * 20, n / (dop * 20));
    if ((n / tuplesPerChunk) < (4 * dop)) {
        tuplesPerChunk = n / dop;
    }

    vectorOfPairs<int, int> chunkPositions(n / tuplesPerChunk, std::make_pair(0, tuplesPerChunk));
    chunkPositions.back().second = n - (((n / tuplesPerChunk) - 1) * (tuplesPerChunk));

    int startIndex = 0;
    for (auto & chunkPosition : chunkPositions) {
        chunkPosition.first = startIndex;
        startIndex += chunkPosition.second;
    }

    std::atomic<int> selectedCount = 0;
    std::atomic<int> chunksCompleted = 0;

    std::vector<SelectIndexesThreadArgs<T>*> threadArgs(dop);

    for (int i = 0; i < dop; ++i) {
        threadArgs[i] = new SelectIndexesThreadArgs<T>;
        threadArgs[i]->chunkPositions = &chunkPositions;
        threadArgs[i]->chunksCompleted = &chunksCompleted;
        threadArgs[i]->maxSize = n;
        threadArgs[i]->inputFilter = inputFilter;
        threadArgs[i]->selection = selection;
        threadArgs[i]->threshold = threshold;
        threadArgs[i]->selectedCount = &selectedCount;
        threadArgs[i]->dop = dop;

        pthread_create(&threads[i], NULL, selectIndexesAdaptiveParallelAux<T>,
                       threadArgs[i]);
    }

    for (int i = 0; i < dop; ++i) {
        pthread_join(threads[i], nullptr);
    }

    return selectedCount;
}

template<typename T1, typename T2>
int selectValuesBranch(int endIndex, const T2 *inputData, const T1 *inputFilter, T2 *selection,
                       T1 threshold, int startIndex) {
    auto k = 0;
    for (auto i = startIndex; i < endIndex; ++i) {
        if (inputFilter[i] <= threshold) {
            selection[k++] = inputData[i];
        }
    }
    return k;
}

template<typename T1, typename T2>
int selectValuesPredication(int endIndex, const T2 *inputData, const T1 *inputFilter, T2 *selection,
                            T1 threshold, int startIndex) {
    auto k = 0;
    for (auto i = startIndex; i < endIndex; ++i) {
        selection[k] = inputData[i];
//        selection[k] = inputData[(0L - (inputFilter[i] <= threshold)) & i];
        k += (inputFilter[i] <= threshold);
    }
    return k;
}


#ifdef __AVX2__

template<typename T1, typename T2>
struct selectValuesVectorizedStruct {
    static int run(int endIndex, const T2 *inputData, const T1 *inputFilter,
                   T2 *selection, T1 threshold, int startIndex) {
        std::cerr << "Non-specialised template for selectValuesVectorized called. Ensure that a specialised template "
                     "has been implemented for the inputFilter data type requested" << std::endl;
        exit(1);
    }
};

template<typename T>
struct selectValuesVectorizedStruct<int, T> {
    static int run(int endIndex, const T *inputData, const int *inputFilter,
                   T *selection, int threshold, int startIndex) {
        auto k = 0;

        // Process unaligned tuples
        auto misalignedCount = 0;
        for (auto i = startIndex; !arrayIsSimd256Aligned(inputFilter + i); ++i) {
            selection[k] = inputData[i];
            k += (inputFilter[i] <= threshold);
            ++misalignedCount;
        }

        // Vectorize the loop for aligned tuples
        int simdWidth = sizeof(__m256i) / sizeof(int);
        int simdIterations = (endIndex - startIndex - misalignedCount) / simdWidth;
        __m256i thresholdVector = _mm256_set1_epi32(threshold);

        for (auto i = startIndex + misalignedCount;
             i < startIndex + misalignedCount + (simdIterations * simdWidth); i += simdWidth) {
            __m256i filterVector = _mm256_load_si256((__m256i *) (inputFilter + i));

            // Compare filterVector <= thresholdVector
            __mmask8 mask = ~_mm256_cmpgt_epi32_mask(filterVector, thresholdVector);

            for (auto j = 0; j < simdWidth; ++j) {
                selection[k] = inputData[i + j];
                k += (mask >> j) & 1;
            }
        }

        // Process any remaining tuples
        for (auto i = startIndex + misalignedCount + simdIterations * simdWidth; i < endIndex; ++i) {
            selection[k] = inputData[i];
            k += (inputFilter[i] <= threshold);
        }

        return k;
    }
};

template<typename T>
struct selectValuesVectorizedStruct<int64_t, T> {
    static int run(int endIndex, const T *inputData, const int64_t *inputFilter, T *selection,
                   int64_t threshold, int startIndex) {
        auto k = 0;

        // Process unaligned tuples
        auto misalignedCount = 0;
        for (auto i = startIndex; !arrayIsSimd256Aligned(inputFilter + i); ++i) {
            selection[k] = inputData[i];
            k += (inputFilter[i] <= threshold);
            ++misalignedCount;
        }

        // Vectorize the loop for aligned tuples
        int simdWidth = sizeof(__m256i) / sizeof(int64_t);
        int simdIterations = (endIndex - startIndex - misalignedCount) / simdWidth;
        __m256i thresholdVector = _mm256_set1_epi64x(threshold);

        for (auto i = startIndex + misalignedCount;
             i < startIndex + misalignedCount + (simdIterations * simdWidth); i += simdWidth) {
            __m256i filterVector = _mm256_load_si256((__m256i *) (inputFilter + i));

            // Compare filterVector <= thresholdVector
            __mmask8 mask = ~_mm256_cmpgt_epi64_mask(filterVector, thresholdVector);

            for (auto j = 0; j < simdWidth; ++j) {
                selection[k] = inputData[i + j];
                k += (mask >> j) & 1;
            }
        }

        // Process any remaining tuples
        for (auto i = startIndex + misalignedCount + simdIterations * simdWidth; i < endIndex; ++i) {
            selection[k] = inputData[i];
            k += (inputFilter[i] <= threshold);
        }

        return k;
    }
};

template<typename T>
struct selectValuesVectorizedStruct<float, T> {
    static int run(int endIndex, const T *inputData, const float *inputFilter, T *selection,
                   float threshold, int startIndex) {
        auto k = 0;

        // Process unaligned tuples
        auto misalignedCount = 0;
        for (auto i = startIndex; !arrayIsSimd256Aligned(inputFilter + i); ++i) {
            selection[k] = inputData[i];
            k += (inputFilter[i] <= threshold);
            ++misalignedCount;
        }

        // Vectorize the loop for aligned tuples
        int simdWidth = sizeof(__m256) / sizeof(float);
        int simdIterations = (endIndex - startIndex - misalignedCount) / simdWidth;
        __m256 thresholdVector = _mm256_set1_ps(threshold);

        for (auto i = startIndex + misalignedCount;
             i < startIndex + misalignedCount + (simdIterations * simdWidth); i += simdWidth) {
            __m256 filterVector = _mm256_load_ps(inputFilter + i);

            // Compare filterVector <= thresholdVector
            __mmask8 mask = _mm256_cmp_ps_mask(filterVector, thresholdVector, _CMP_LE_OQ);

            for (auto j = 0; j < simdWidth; ++j) {
                selection[k] = inputData[i + j];
                k += (mask >> j) & 1;
            }
        }

        // Process any remaining tuples
        for (auto i = startIndex + misalignedCount + simdIterations * simdWidth; i < endIndex; ++i) {
            selection[k] = inputData[i];
            k += (inputFilter[i] <= threshold);
        }

        return k;
    }
};

template<typename T1, typename T2>
int selectValuesVectorized(int endIndex, const T2 *inputData, const T1 *inputFilter, T2 *selection,
                           T1 threshold, int startIndex) {
    return selectValuesVectorizedStruct<T1,T2>::run(endIndex, inputData, inputFilter, selection, threshold, startIndex);
}

#else

template<typename T1, typename T2>
struct selectValuesVectorizedStruct {
    static int run(int endIndex, const T2 *inputData, const T1 *inputFilter,
                   T2 *selection, T1 threshold, int startIndex) {
        std::cerr << "Non-specialised template for selectValuesVectorized called. Ensure that a specialised template "
                     "has been implemented for the inputFilter data type requested" << std::endl;
        exit(1);
    }
};

template<typename T>
struct selectValuesVectorizedStruct<int, T> {
    static int run(int endIndex, const T *inputData, const int *inputFilter,
                   T *selection, int threshold, int startIndex) {
        auto k = 0;

        // Process unaligned tuples
        auto misalignedCount = 0;
        for (auto i = startIndex; !arrayIsSimd128Aligned(inputFilter + i); ++i) {
            selection[k] = inputData[i];
            k += (inputFilter[i] <= threshold);
            ++misalignedCount;
        }

        // Vectorize the loop for aligned tuples
        int simdWidth = sizeof(__m128i) / sizeof(int);
        int simdIterations = (endIndex - startIndex - misalignedCount) / simdWidth;
        __m128i thresholdVector = _mm_set1_epi32(threshold);

        for (auto i = startIndex + misalignedCount;
             i < startIndex + misalignedCount + (simdIterations * simdWidth); i += simdWidth) {
            __m128i filterVector = _mm_load_si128((__m128i *) (inputFilter + i));

            // Compare filterVector <= thresholdVector
            __m128i cmpResult = _mm_cmpgt_epi32(filterVector, thresholdVector);
            int mask = ~_mm_movemask_epi8(cmpResult);

            for (auto j = 0; j < simdWidth; ++j) {
                selection[k] = inputData[i + j];
                k += (mask >> (j * 4)) & 1;
            }
        }

        // Process any remaining tuples
        for (auto i = startIndex + misalignedCount + simdIterations * simdWidth; i < endIndex; ++i) {
            selection[k] = inputData[i];
            k += (inputFilter[i] <= threshold);
        }

        return k;
    }
};

template<typename T>
struct selectValuesVectorizedStruct<int64_t, T> {
    static int run(int endIndex, const T *inputData, const int64_t *inputFilter, T *selection,
                   int64_t threshold, int startIndex) {
        auto k = 0;

        // Process unaligned tuples
        auto misalignedCount = 0;
        for (auto i = startIndex; !arrayIsSimd128Aligned(inputFilter + i); ++i) {
            selection[k] = inputData[i];
            k += (inputFilter[i] <= threshold);
            ++misalignedCount;
        }

        // Vectorize the loop for aligned tuples
        int simdWidth = sizeof(__m128i) / sizeof(int64_t);
        int simdIterations = (endIndex - startIndex - misalignedCount) / simdWidth;
        __m128i thresholdVector = _mm_set1_epi64x(threshold);

        for (auto i = startIndex + misalignedCount;
             i < startIndex + misalignedCount + (simdIterations * simdWidth); i += simdWidth) {
            __m128i filterVector = _mm_load_si128((__m128i *) (inputFilter + i));

            // Compare filterVector <= thresholdVector
            __m128i cmpResult = _mm_cmpgt_epi64(filterVector, thresholdVector);
            int mask = ~_mm_movemask_epi8(cmpResult);

            for (auto j = 0; j < simdWidth; ++j) {
                selection[k] = inputData[i + j];
                k += (mask >> (j * 8)) & 1;
            }
        }

        // Process any remaining tuples
        for (auto i = startIndex + misalignedCount + simdIterations * simdWidth; i < endIndex; ++i) {
            selection[k] = inputData[i];
            k += (inputFilter[i] <= threshold);
        }

        return k;
    }
};

template<typename T1, typename T2>
int selectValuesVectorized(int endIndex, const T2 *inputData, const T1 *inputFilter, T2 *selection,
                           T1 threshold, int startIndex) {
    return selectValuesVectorizedStruct<T1,T2>::run(endIndex, inputData, inputFilter, selection, threshold, startIndex);
}

#endif

template<typename T1, typename T2>
inline int runSelectValuesChunk(SelectValuesChoice selectValuesChoice,
                                int tuplesToProcess,
                                int &startIndex,
                                int &n,
                                const T2 *inputData,
                                const T1 *inputFilter,
                                T2 *&selection,
                                T1 threshold,
                                int &k,
                                int &consecutivePredications) {
    int selected;
    if (selectValuesChoice == SelectValuesChoice::ValuesBranch) {
        Counters::getInstance().readSharedEventSet();
        selected = selectValuesBranch(startIndex + tuplesToProcess, inputData, inputFilter,
                                      selection, threshold, startIndex);
        Counters::getInstance().readSharedEventSet();
    } else {
        Counters::getInstance().readSharedEventSet();
        selected = selectValuesVectorized(startIndex + tuplesToProcess, inputData, inputFilter,
                                          selection, threshold, startIndex);
        Counters::getInstance().readSharedEventSet();
    }
    n -= tuplesToProcess;
    startIndex += tuplesToProcess;
    selection += selected;
    k += selected;
    consecutivePredications += (selectValuesChoice == SelectValuesChoice::ValuesVectorized);
    return selected;
}

inline void performSelectValuesAdaption(SelectValuesChoice &selectValuesChoice,
                                        const long_long *counterValues,
                                        float branchCrossoverSelectivity,
                                        float vectorizationCrossoverSelectivity,
                                        float branchCrossoverBranchMisses,
                                        float selectivity,
                                        int &consecutiveVectorized) {

    if (__builtin_expect((static_cast<float>(counterValues[0]) > branchCrossoverBranchMisses || selectivity >
        vectorizationCrossoverSelectivity) && selectValuesChoice == SelectValuesChoice::ValuesBranch, false)) {
        selectValuesChoice = SelectValuesChoice::ValuesVectorized;
    }

    if (__builtin_expect(selectivity < branchCrossoverSelectivity
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

    std::string mispredictionsMachineConstantName = "SelectValuesLowerMispredictions_" + std::to_string(sizeof(T1)) +
                                                    "B_inputFilter_" + std::to_string(sizeof(T2)) + "B_inputData_1_dop";
    std::string selectivityMachineConstantName = "SelectValuesLowerSelectivity_" + std::to_string(sizeof(T1)) +
                                                 "B_inputFilter_" + std::to_string(sizeof(T2)) + "B_inputData_1_dop";
    float branchCrossoverSelectivity = MachineConstants::getInstance().getMachineConstant(mispredictionsMachineConstantName);
    float vectorizationCrossoverSelectivity = MachineConstants::getInstance().getMachineConstant(selectivityMachineConstantName);

    // Equation below are only valid at the extreme ends of selectivity
    float branchCrossoverBranchMisses = branchCrossoverSelectivity * static_cast<float>(tuplesPerAdaption);

    // Modified values for short branch burst chunks
    float branchCrossoverBranchMisses_BranchBurst = branchCrossoverSelectivity * static_cast<float>(tuplesInBranchBurst);

    int startIndex = 0;
    int k = 0;
    int consecutiveVectorized = 0;
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
            selected = runSelectValuesChunk<T1,T2>(selectValuesChoice, tuplesToProcess, startIndex, n,
                                            inputData, inputFilter, selection, threshold, k,
                                            consecutiveVectorized);
            performSelectValuesAdaption(selectValuesChoice, counterValues, branchCrossoverSelectivity,
                                        vectorizationCrossoverSelectivity,
                                        branchCrossoverBranchMisses_BranchBurst,
                                        static_cast<float>(selected) / static_cast<float>(tuplesInBranchBurst),
                                        consecutiveVectorized);
        } else {
            tuplesToProcess = std::min(n, tuplesPerAdaption);
            selected = runSelectValuesChunk<T1,T2>(selectValuesChoice, tuplesToProcess, startIndex,n,
                                            inputData, inputFilter, selection, threshold, k,
                                            consecutiveVectorized);
            performSelectValuesAdaption(selectValuesChoice, counterValues, branchCrossoverSelectivity,
                                        vectorizationCrossoverSelectivity,
                                        branchCrossoverBranchMisses,
                                        static_cast<float>(selected) / static_cast<float>(tuplesPerAdaption),
                                        consecutiveVectorized);
        }
    }

    return k;
}

template<typename T1, typename T2>
struct SelectValuesThreadArgs {
    vectorOfPairs<int, int> *chunkPositions;
    std::atomic<int> *chunksCompleted;
    int maxSize;
    const T2* inputData;
    const T1* inputFilter;
    T2 *selection;
    T1 threshold;
    std::atomic<int> *selectedCount;
    int dop;
};

template<typename T1, typename T2>
inline int runSelectValuesChunkParallel(SelectValuesChoice selectValuesChoice,
                                        int tuplesToProcess,
                                        int &startIndex,
                                        int &n,
                                        const T2 *inputData,
                                        const T1 *inputFilter,
                                        T2 *&selection,
                                        T1 threshold,
                                        int &k,
                                        int &consecutivePredications,
                                        int eventSet,
                                        long_long *counterValues) {
    int selected;
    if (selectValuesChoice == SelectValuesChoice::ValuesBranch) {
        readThreadEventSet(eventSet, 1, counterValues);
        selected = selectValuesBranch(startIndex + tuplesToProcess, inputData, inputFilter, selection,
                                      threshold, startIndex);
        readThreadEventSet(eventSet, 1, counterValues);
    } else {
        readThreadEventSet(eventSet, 1, counterValues);
        selected = selectValuesVectorized(startIndex + tuplesToProcess, inputData, inputFilter, selection,
                                          threshold, startIndex);
        readThreadEventSet(eventSet, 1, counterValues);
    }
    n -= tuplesToProcess;
    startIndex += tuplesToProcess;
    selection += selected;
    k += selected;
    consecutivePredications += (selectValuesChoice == SelectValuesChoice::ValuesVectorized);
    return selected;
}

template<typename T1, typename T2>
void *selectValuesAdaptiveParallelAux(void *arg) {
    auto* args = static_cast<SelectValuesThreadArgs<T1, T2>*>(arg);
    vectorOfPairs<int, int> *chunkPositions = args->chunkPositions;
    std::atomic<int> *chunksCompleted = args->chunksCompleted;
    int maxSize = args->maxSize;
    const T2 *inputData = args->inputData;
    const T1 *inputFilter = args->inputFilter;
    T2 *overallSelection = args->selection;
    T1 threshold = args->threshold;
    std::atomic<int> *selectedCount = args->selectedCount;
    int dop = args->dop;

    auto threadSelection = new T2[maxSize];
    auto threadSelectionStart = threadSelection;

    auto tuplesPerAdaption = 50000;
    auto maxConsecutiveVectorized = 10;
    auto tuplesInBranchBurst = 1000;

    std::string mispredictionsMachineConstantName = "SelectValuesLowerMispredictions_" + std::to_string(sizeof(T1)) +
                 "B_inputFilter_" + std::to_string(sizeof(T2)) + "B_inputData_" + std::to_string(dop) + "_dop";
    std::string selectivityMachineConstantName = "SelectValuesLowerSelectivity_" + std::to_string(sizeof(T1)) +
                 "B_inputFilter_" + std::to_string(sizeof(T2)) + "B_inputData_" + std::to_string(dop) + "_dop";
    float branchCrossoverSelectivity = MachineConstants::getInstance().getMachineConstant(mispredictionsMachineConstantName);
    float vectorizationCrossoverSelectivity = MachineConstants::getInstance().getMachineConstant(selectivityMachineConstantName);

    // Equation below are only valid at the extreme ends of selectivity
    float branchCrossoverBranchMisses = branchCrossoverSelectivity * static_cast<float>(tuplesPerAdaption);

    // Modified values for short branch burst chunks
    float branchCrossoverBranchMisses_BranchBurst = branchCrossoverSelectivity * static_cast<float>(tuplesInBranchBurst);

    int eventSet = PAPI_NULL;
    std::vector<std::string> counters = {"PERF_COUNT_HW_BRANCH_MISSES"};
    long_long counterValues[1] = {0};
    createThreadEventSet(&eventSet, counters);

    int nextChunk = (*chunksCompleted).fetch_add(1);
    int kTotal = 0;
    int startIndex, n, k, consecutiveVectorized, tuplesToProcess, selected;

    while (nextChunk < static_cast<int>(chunkPositions->size())) {

        startIndex = (*chunkPositions)[nextChunk].first;
        n = (*chunkPositions)[nextChunk].second;
        k = 0;
        consecutiveVectorized = 0;
        SelectValuesChoice selectValuesChoice = SelectValuesChoice::ValuesBranch;

        while (n > 0) {

            if (__builtin_expect(consecutiveVectorized == maxConsecutiveVectorized, false)) {
                selectValuesChoice = SelectValuesChoice::ValuesVectorized;
                consecutiveVectorized = 0;
                tuplesToProcess = std::min(n, tuplesInBranchBurst);
                selected = runSelectValuesChunkParallel<T1, T2>(selectValuesChoice, tuplesToProcess,
                                                                startIndex, n, inputData, inputFilter,
                                                                threadSelection, threshold, k,
                                                                consecutiveVectorized, eventSet, counterValues);
                performSelectValuesAdaption(selectValuesChoice, counterValues, branchCrossoverSelectivity,
                                            vectorizationCrossoverSelectivity,
                                            branchCrossoverBranchMisses_BranchBurst,
                                            static_cast<float>(selected) / static_cast<float>(tuplesInBranchBurst),
                                            consecutiveVectorized);
            } else {
                tuplesToProcess = std::min(n, tuplesPerAdaption);
                selected = runSelectValuesChunkParallel<T1, T2>(selectValuesChoice, tuplesToProcess,
                                                                startIndex, n, inputData, inputFilter,
                                                                threadSelection, threshold, k,
                                                                consecutiveVectorized, eventSet, counterValues);
                performSelectValuesAdaption(selectValuesChoice, counterValues, branchCrossoverSelectivity,
                                            vectorizationCrossoverSelectivity,
                                            branchCrossoverBranchMisses,
                                            static_cast<float>(selected) / static_cast<float>(tuplesPerAdaption),
                                            consecutiveVectorized);
            }
        }

        kTotal += k;
        nextChunk = (*chunksCompleted).fetch_add(1);
    }

    destroyThreadEventSet(eventSet, counterValues);

    int overallSelectionStartIndex = (*selectedCount).fetch_add(kTotal);
    memcpy(overallSelection + overallSelectionStartIndex, threadSelectionStart, kTotal * sizeof(int));

    delete []threadSelectionStart;
    delete args;

    return nullptr;
}

template<typename T1, typename T2>
int selectValuesAdaptiveParallel(int n, const T2 *inputData, const T1 *inputFilter, T2 *selection, T1 threshold, int dop) {
    assert(1 < dop && dop <= maxDop());

    Counters::getInstance();
    MachineConstants::getInstance();
    pthread_t threads[dop];

    int adaptivePeriod = 50000;
    int tuplesPerChunk = std::max(adaptivePeriod * 20, n / (dop * 20));
    if ((n / tuplesPerChunk) < (4 * dop)) {
        tuplesPerChunk = n / dop;
    }

    vectorOfPairs<int, int> chunkPositions(n / tuplesPerChunk, std::make_pair(0, tuplesPerChunk));
    chunkPositions.back().second = n - (((n / tuplesPerChunk) - 1) * (tuplesPerChunk));

    int startIndex = 0;
    for (auto & chunkPosition : chunkPositions) {
        chunkPosition.first = startIndex;
        startIndex += chunkPosition.second;
    }

    std::atomic<int> selectedCount = 0;
    std::atomic<int> chunksCompleted = 0;

    std::vector<SelectValuesThreadArgs<T1, T2>*> threadArgs(dop);

    for (int i = 0; i < dop; ++i) {
        threadArgs[i] = new SelectValuesThreadArgs<T1, T2>;
        threadArgs[i]->chunkPositions = &chunkPositions;
        threadArgs[i]->chunksCompleted = &chunksCompleted;
        threadArgs[i]->maxSize = n;
        threadArgs[i]->inputData = inputData;
        threadArgs[i]->inputFilter = inputFilter;
        threadArgs[i]->selection = selection;
        threadArgs[i]->threshold = threshold;
        threadArgs[i]->selectedCount = &selectedCount;
        threadArgs[i]->dop = dop;

        pthread_create(&threads[i], NULL, selectValuesAdaptiveParallelAux<T1, T2>,
                       threadArgs[i]);
    }

    for (int i = 0; i < dop; ++i) {
        pthread_join(threads[i], nullptr);
    }

    return selectedCount;
}

template<typename T1, typename T2>
struct runSelectFunctionStruct { static int run (Select selectImplementation,
                                                 int n, const T2 *inputData, const T1 *inputFilter,
                                                 T2 *selection, T1 threshold, int dop) {
        switch (selectImplementation) {
            case Select::ImplementationValuesBranch:
                return selectValuesBranch(n, inputData, inputFilter, selection, threshold);
            case Select::ImplementationValuesPredication:
                return selectValuesPredication(n, inputData, inputFilter, selection, threshold);
            case Select::ImplementationValuesVectorized:
                return selectValuesVectorized(n, inputData, inputFilter, selection, threshold);
            case Select::ImplementationValuesAdaptive:
                return selectValuesAdaptive(n, inputData, inputFilter, selection, threshold);
            case Select::ImplementationValuesAdaptiveParallel:
                return selectValuesAdaptiveParallel(n, inputData, inputFilter, selection, threshold, dop);
            default:
                std::cout << "Invalid selection of 'Select' implementation!" << std::endl;
                exit(1);
        }
    }
};

template<typename T>
struct runSelectFunctionStruct<T, int> { static int run (Select selectImplementation,
                                                         int n, const int *inputData, const T *inputFilter,
                                                         int *selection, T threshold, int dop) {
        switch (selectImplementation) {
            case Select::ImplementationIndexesBranch:
                return selectIndexesBranch(n, inputFilter, selection, threshold);
            case Select::ImplementationIndexesPredication:
                return selectIndexesPredication(n, inputFilter, selection, threshold);
            case Select::ImplementationIndexesAdaptive:
                return selectIndexesAdaptive(n, inputFilter, selection, threshold);
            case Select::ImplementationIndexesAdaptiveParallel:
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
                return selectValuesAdaptiveParallel(n, inputData, inputFilter, selection, threshold, dop);
            default:
                std::cout << "Invalid selection of 'Select' implementation!" << std::endl;
                exit(1);
        }
    }
};

template<typename T1, typename T2>
int runSelectFunction(Select selectImplementation,
                      int n, const T2 *inputData, const T1 *inputFilter, T2 *selection, T1 threshold, int dop) {
    return runSelectFunctionStruct<T1,T2>::run(selectImplementation, n, inputData, inputFilter, selection, threshold, dop);
}


}

#endif //MABPL_SELECT_IMPLEMENTATION_H

#ifndef HAQP_SELECT_IMPLEMENTATION_H
#define HAQP_SELECT_IMPLEMENTATION_H

#include <immintrin.h>
#include <functional>
#include <cassert>
#include <atomic>
#include <cstring>

#include "utilities/papiWrapper.h"
#include "utilities/systemInformation.h"
#include "utilities/dataStructures.h"
#include "machine_constants/machineConstants.h"


namespace HAQP {

/****************************** FOUNDATIONAL ALGORITHMS ******************************/

template <typename T>
class SelectIndexesImplementation {
public:
    virtual inline int processMicroBatch(int endIndex, const T *inputFilter, int *selection, T threshold,
                                         int startIndex) = 0;
    virtual inline int processMicroBatch(int endIndex, const T *inputFilter, int *selection, T threshold) = 0;
};

template <typename T>
class SelectIndexesBranch : public SelectIndexesImplementation<T> {
public:
    inline int processMicroBatch(int endIndex, const T *inputFilter, int *selection, T threshold,
                                 int startIndex) final {
        auto k = 0;
        for (auto i = startIndex; i < endIndex; ++i) {
            if (inputFilter[i] <= threshold) {
                selection[k++] = i;
            }
        }
        return k;
    }

    inline int processMicroBatch(int endIndex, const T *inputFilter, int *selection, T threshold) final {
        return processMicroBatch(endIndex, inputFilter, selection, threshold, 0);
    }

};

template <typename T>
class SelectIndexesPredication : public SelectIndexesImplementation<T> {
public:
    inline int processMicroBatch(int endIndex, const T *inputFilter, int *selection, T threshold,
                                 int startIndex) final {
        auto k = 0;
        for (auto i = startIndex; i < endIndex; ++i) {
            selection[k] = i;
            k += (inputFilter[i] <= threshold);
        }
        return k;
    }

    inline int processMicroBatch(int endIndex, const T *inputFilter, int *selection, T threshold) final {
        return processMicroBatch(endIndex, inputFilter, selection, threshold, 0);
    }
};

/****************************** SINGLE-THREADED ******************************/

template<typename T>
class MonitorSelectIndexes;

template<typename T>
class SelectIndexesAdaptive {
public:
    SelectIndexesAdaptive(int n_, const T *inputFilter_, int *selection_, T threshold_);
    void adjustRobustness(int adjustment);
    int processInput();

private:
    inline void processMicroBatch();

    int remainingTuples;
    const T *inputFilter;
    int *selection;
    T threshold;

    int tuplesPerHazardCheck;
    int maxConsecutivePredications;
    int tuplesInBranchBurst;

    int microBatchStartIndex;
    int totalSelected;
    int consecutivePredications;
    SelectIndexesOperators activeOperator;

    MonitorSelectIndexes<T> monitor;
    SelectIndexesBranch<T> branchOperator;
    SelectIndexesPredication<T> predicationOperator;

    int microBatchSize{};
    int microBatchSelected{};
};

template<typename T>
class MonitorSelectIndexes {
public:
    explicit MonitorSelectIndexes(SelectIndexesAdaptive<T> *selectOperator_) : selectOperator(selectOperator_){
        std::vector<std::string> counters = {"PERF_COUNT_HW_BRANCH_MISSES"};
        branchMispredictions = Counters::getInstance().getSharedEventSetEvents(counters);

        std::string lowerName = "SelectIndexesLower_" + std::to_string(sizeof(T)) + "B_inputFilter_1_dop";
        std::string upperName = "SelectIndexesUpper_" + std::to_string(sizeof(T)) + "B_inputFilter_1_dop";
        lowerCrossoverSelectivity = static_cast<float>(MachineConstants::getInstance().getMachineConstant(lowerName));
        upperCrossoverSelectivity = static_cast<float>(MachineConstants::getInstance().getMachineConstant(upperName));

        // Gradient of number of branch misses between lower cross-over selectivity and upper cross-over selectivity
        m = ((1 - upperCrossoverSelectivity) - lowerCrossoverSelectivity) /
                  (upperCrossoverSelectivity - lowerCrossoverSelectivity);
    }

    void checkHazards(int n, int selected) {
        float selectivity = static_cast<float>(selected) / static_cast<float>(n);

        if (__builtin_expect((static_cast<float>(*branchMispredictions) / static_cast<float>(n)) >
                              (((selectivity - lowerCrossoverSelectivity) * m) + lowerCrossoverSelectivity), false)) {
            selectOperator->adjustRobustness(1);
        }

        if (__builtin_expect((selectivity < lowerCrossoverSelectivity) ||
                             (selectivity > upperCrossoverSelectivity), false)) {
            selectOperator->adjustRobustness(-1);
        }
    }

private:
    const long_long *branchMispredictions;
    float lowerCrossoverSelectivity;
    float upperCrossoverSelectivity;
    float m;
    SelectIndexesAdaptive<T> *selectOperator;
};

template<typename T>
SelectIndexesAdaptive<T>::SelectIndexesAdaptive(int n_, const T *inputFilter_, int *selection_, T threshold_) :
    remainingTuples(n_),
    inputFilter(inputFilter_),
    selection(selection_),
    threshold(threshold_),
    tuplesPerHazardCheck(50000),
    maxConsecutivePredications(10),
    tuplesInBranchBurst(1000),
    microBatchStartIndex(0),
    totalSelected(0),
    consecutivePredications(0),
    activeOperator(SelectIndexesOperators::IndexesPredication),
    monitor(MonitorSelectIndexes<T>(this)),
    branchOperator(SelectIndexesBranch<T>()),
    predicationOperator(SelectIndexesPredication<T>()) {}

template<typename T>
void SelectIndexesAdaptive<T>::adjustRobustness(int adjustment) {
    if (__builtin_expect((adjustment > 0) &&
                         activeOperator == SelectIndexesOperators::IndexesBranch, false)) {
//            std::cout << "Switched to select predication" << std::endl;
        activeOperator = SelectIndexesOperators::IndexesPredication;
    } else if (__builtin_expect((adjustment < 0) &&
                                activeOperator == SelectIndexesOperators::IndexesPredication, false)) {
//            std::cout << "Switched to select branch" << std::endl;
        activeOperator = SelectIndexesOperators::IndexesBranch;
        consecutivePredications = 0;
    }
}

template<typename T>
int SelectIndexesAdaptive<T>::processInput() {
    while (remainingTuples > 0) {
        if (__builtin_expect(consecutivePredications == maxConsecutivePredications, false)) {
//            std::cout << "Running branch burst" << std::endl;
            activeOperator = SelectIndexesOperators::IndexesBranch;
            consecutivePredications = 0;
            microBatchSize = std::min(remainingTuples, tuplesInBranchBurst);
        } else {
            microBatchSize = std::min(remainingTuples, tuplesPerHazardCheck);
        }
        processMicroBatch();
        monitor.checkHazards(microBatchSize, microBatchSelected);
    }

    return totalSelected;
}

template<typename T>
inline void SelectIndexesAdaptive<T>::processMicroBatch() {
    microBatchSelected = 0;
    if (activeOperator == SelectIndexesOperators::IndexesBranch) {
        Counters::getInstance().readSharedEventSet();
        microBatchSelected = branchOperator.processMicroBatch(microBatchStartIndex + microBatchSize, inputFilter,
                                                              selection, threshold, microBatchStartIndex);
        Counters::getInstance().readSharedEventSet();
    } else {
        Counters::getInstance().readSharedEventSet();
        microBatchSelected = predicationOperator.processMicroBatch(microBatchStartIndex + microBatchSize, inputFilter,
                                                                   selection, threshold, microBatchStartIndex);
        Counters::getInstance().readSharedEventSet();
        consecutivePredications++;
    }
    remainingTuples -= microBatchSize;
    microBatchStartIndex += microBatchSize;
    selection += microBatchSelected;
    totalSelected += microBatchSelected;
}

/****************************** MULTI-THREADED ******************************/

template<typename T>
struct SelectIndexesThreadArgs {
    vectorOfPairs<int, int> *taskIndexes;
    std::atomic<int> *tasksCompleted;
    int maxSize;
    const T* inputFilter;
    int *selection;
    T threshold;
    std::atomic<int> *selectedCount;
    int dop;
};

template<typename T>
class MonitorSelectIndexesParallel;

template<typename T>
class SelectIndexesAdaptiveParallelAux {
public:
    explicit SelectIndexesAdaptiveParallelAux(SelectIndexesThreadArgs<T>* args);
    void adjustRobustness(int adjustment);
    int processInput();
    void mergeOutput();
    ~SelectIndexesAdaptiveParallelAux();

private:
    inline void processMicroBatch();

    const T *inputFilter;
    int *threadSelection;
    int *threadSelectionStart;
    int *overallSelection;
    T threshold;

    int tuplesPerHazardCheck;
    int maxConsecutivePredications;
    int tuplesInBranchBurst;

    SelectIndexesOperators activeOperator;

    SelectIndexesBranch<T> branchOperator;
    SelectIndexesPredication<T> predicationOperator;

    vectorOfPairs<int, int> *taskIndexes;
    std::atomic<int> *tasksCompleted;
    std::atomic<int> *selectedCount;

    int dop;
    long_long *branchMispredictions;
    MonitorSelectIndexesParallel<T> *monitor;
    int eventSet;

    int microBatchStartIndex{};
    int threadSelected{};
    int consecutivePredications{};
    int microBatchSize{};
    int microBatchSelected{};
    int taskSelected{};
    int taskTuplesRemaining{};
};

template<typename T>
class MonitorSelectIndexesParallel {
public:
    MonitorSelectIndexesParallel(SelectIndexesAdaptiveParallelAux<T> *selectOperator_, int dop,
                                 long_long *branchMispredictions_) :
                                 branchMispredictions(branchMispredictions_),
                                 selectOperator(selectOperator_) {

        std::string lowerName = "SelectIndexesLower_" + std::to_string(sizeof(T)) + "B_inputFilter_" +
                                std::to_string(dop) + "_dop";
        std::string upperName = "SelectIndexesUpper_" + std::to_string(sizeof(T)) + "B_inputFilter_" +
                                std::to_string(dop) + "_dop";
        lowerCrossoverSelectivity = static_cast<float>(MachineConstants::getInstance().getMachineConstant(lowerName));
        upperCrossoverSelectivity = static_cast<float>(MachineConstants::getInstance().getMachineConstant(upperName));

        // Gradient of number of branch misses between lower cross-over selectivity and upper cross-over selectivity
        m = ((1 - upperCrossoverSelectivity) - lowerCrossoverSelectivity) /
            (upperCrossoverSelectivity - lowerCrossoverSelectivity);
    }

    void checkHazards(int n, int selected) {
        float selectivity = static_cast<float>(selected) / static_cast<float>(n);

        if (__builtin_expect((static_cast<float>(*branchMispredictions) / static_cast<float>(n)) >
                             (((selectivity - lowerCrossoverSelectivity) * m) + lowerCrossoverSelectivity), false)) {
            selectOperator->adjustRobustness(1);
        }

        if (__builtin_expect((selectivity < lowerCrossoverSelectivity) ||
                             (selectivity > upperCrossoverSelectivity), false)) {
            selectOperator->adjustRobustness(-1);
        }
    }

private:
    long_long *branchMispredictions;
    float lowerCrossoverSelectivity;
    float upperCrossoverSelectivity;
    float m;
    SelectIndexesAdaptiveParallelAux<T> *selectOperator;
};

template<typename T>
SelectIndexesAdaptiveParallelAux<T>::SelectIndexesAdaptiveParallelAux(SelectIndexesThreadArgs<T>* args) :
        tuplesPerHazardCheck(50000),
        maxConsecutivePredications(10),
        tuplesInBranchBurst(1000),
        activeOperator(SelectIndexesOperators::IndexesPredication),
        branchOperator(SelectIndexesBranch<T>()),
        predicationOperator(SelectIndexesPredication<T>()) {

    threadSelection = new int[args->maxSize];
    threadSelectionStart = threadSelection;

    inputFilter = args->inputFilter;
    overallSelection = args->selection;
    threshold = args->threshold;
    dop = args->dop;

    taskIndexes = args->taskIndexes;
    tasksCompleted = args->tasksCompleted;
    selectedCount = args->selectedCount;

    delete args;

    eventSet = PAPI_NULL;
    std::vector<std::string> counters = {"PERF_COUNT_HW_BRANCH_MISSES"};
    createThreadEventSet(&eventSet, counters);

    branchMispredictions = new long_long;
    monitor = new MonitorSelectIndexesParallel<T>(this, dop, branchMispredictions);
}

template<typename T>
void SelectIndexesAdaptiveParallelAux<T>::adjustRobustness(int adjustment) {
    if (__builtin_expect((adjustment > 0) &&
                         activeOperator == SelectIndexesOperators::IndexesBranch, false)) {
//            std::cout << "Switched to select predication" << std::endl;
        activeOperator = SelectIndexesOperators::IndexesPredication;
    } else if (__builtin_expect((adjustment < 0) &&
                                activeOperator == SelectIndexesOperators::IndexesPredication, false)) {
//            std::cout << "Switched to select branch" << std::endl;
        activeOperator = SelectIndexesOperators::IndexesBranch;
        consecutivePredications = 0;
    }
}

template<typename T>
int SelectIndexesAdaptiveParallelAux<T>::processInput() {
    int nextTaskNumber = (*tasksCompleted).fetch_add(1);

    while (nextTaskNumber < static_cast<int>(taskIndexes->size())) {

        microBatchStartIndex = (*taskIndexes)[nextTaskNumber].first;
        taskTuplesRemaining = (*taskIndexes)[nextTaskNumber].second;
        taskSelected = 0;
        consecutivePredications = 0;
        activeOperator = SelectIndexesOperators::IndexesPredication;

        while (taskTuplesRemaining > 0) {
            if (__builtin_expect(consecutivePredications == maxConsecutivePredications, false)) {
                activeOperator = SelectIndexesOperators::IndexesBranch;
                consecutivePredications = 0;
                microBatchSize = std::min(taskTuplesRemaining, tuplesInBranchBurst);

            } else {
                microBatchSize = std::min(taskTuplesRemaining, tuplesPerHazardCheck);

            }
            processMicroBatch();
            monitor->checkHazards(microBatchSize, microBatchSelected);
        }

        threadSelected += taskSelected;
        nextTaskNumber = (*tasksCompleted).fetch_add(1);
    }

    return threadSelected;
}

template<typename T>
inline void SelectIndexesAdaptiveParallelAux<T>::processMicroBatch() {
    microBatchSelected = 0;
    if (activeOperator == SelectIndexesOperators::IndexesBranch) {
        readThreadEventSet(eventSet, 1, branchMispredictions);
        microBatchSelected = branchOperator.processMicroBatch(microBatchStartIndex + microBatchSize, inputFilter,
                                                              threadSelection, threshold, microBatchStartIndex);
        readThreadEventSet(eventSet, 1, branchMispredictions);
    } else {
        readThreadEventSet(eventSet, 1, branchMispredictions);
        microBatchSelected = predicationOperator.processMicroBatch(microBatchStartIndex + microBatchSize, inputFilter,
                                                                   threadSelection, threshold, microBatchStartIndex);
        readThreadEventSet(eventSet, 1, branchMispredictions);
        consecutivePredications++;
    }
    taskTuplesRemaining -= microBatchSize;
    microBatchStartIndex += microBatchSize;
    threadSelection += microBatchSelected;
    taskSelected += microBatchSelected;
}

template<typename T>
void SelectIndexesAdaptiveParallelAux<T>::mergeOutput() {
    int overallSelectionStartIndex = (*selectedCount).fetch_add(threadSelected);
    memcpy(overallSelection + overallSelectionStartIndex, threadSelectionStart, threadSelected * sizeof(int));
}

template<typename T>
SelectIndexesAdaptiveParallelAux<T>::~SelectIndexesAdaptiveParallelAux() {
    delete []threadSelectionStart;
    destroyThreadEventSet(eventSet, branchMispredictions);
    delete branchMispredictions;
    delete monitor;
}

template<typename T>
void *selectIndexesAdaptiveParallelAux(void *arg) {
    auto* args = static_cast<SelectIndexesThreadArgs<T>*>(arg);
    SelectIndexesAdaptiveParallelAux<T> selectOperator(args);

    selectOperator.processInput();
    selectOperator.mergeOutput();

    return nullptr;
}

template<typename T>
class SelectIndexesAdaptiveParallel {
public:
    SelectIndexesAdaptiveParallel(int n_, const T *inputFilter_, int *selection_, T threshold_, int dop_) :
            n(n_),
            inputFilter(inputFilter_),
            selection(selection_),
            threshold(threshold_),
            dop(dop_) {
        assert(1 < dop && dop <= logicalCoresCount());
        Counters::getInstance();
        MachineConstants::getInstance();
    }

    int processInput() {
        pthread_t threads[dop];

        int adaptivePeriod = 50000;
        int tuplesPerTask = std::max(adaptivePeriod * 20, n / (dop * 20));
        if ((n / tuplesPerTask) < (4 * dop)) {
            tuplesPerTask = n / dop;
        }

        vectorOfPairs<int, int> taskIndexes(n / tuplesPerTask, std::make_pair(0, tuplesPerTask));
        taskIndexes.back().second = n - (((n / tuplesPerTask) - 1) * (tuplesPerTask));

        int startIndex = 0;
        for (auto & taskIndex : taskIndexes) {
            taskIndex.first = startIndex;
            startIndex += taskIndex.second;
        }

        std::atomic<int> selectedCount = 0;
        std::atomic<int> tasksCompleted = 0;

        std::vector<SelectIndexesThreadArgs<T>*> threadArgs(dop);

        for (auto i = 0; i < dop; ++i) {
            threadArgs[i] = new SelectIndexesThreadArgs<T>;
            threadArgs[i]->taskIndexes = &taskIndexes;
            threadArgs[i]->tasksCompleted = &tasksCompleted;
            threadArgs[i]->maxSize = n;
            threadArgs[i]->inputFilter = inputFilter;
            threadArgs[i]->selection = selection;
            threadArgs[i]->threshold = threshold;
            threadArgs[i]->selectedCount = &selectedCount;
            threadArgs[i]->dop = dop;

            pthread_create(&threads[i], NULL, selectIndexesAdaptiveParallelAux<T>, threadArgs[i]);
        }

        for (int i = 0; i < dop; ++i) {
            pthread_join(threads[i], nullptr);
        }

        return selectedCount;
    }

private:
    int n;
    const T *inputFilter;
    int *selection;
    T threshold;
    int dop;
};

/****************************** FOUNDATIONAL ALGORITHMS ******************************/

template<typename T1, typename T2>
class SelectValuesImplementation {
public:
    virtual inline int processMicroBatch(int endIndex, const T2 *inputData, const T1 *inputFilter, T2 *selection,
                                         T1 threshold, int startIndex) = 0;
    virtual inline int processMicroBatch(int endIndex, const T2 *inputData, const T1 *inputFilter, T2 *selection,
                                         T1 threshold) = 0;
};

template<typename T1, typename T2>
class SelectValuesBranch : public SelectValuesImplementation<T1, T2> {
public:
    inline int processMicroBatch(int endIndex, const T2 *inputData, const T1 *inputFilter, T2 *selection,
                                 T1 threshold, int startIndex) final {
        auto k = 0;
        for (auto i = startIndex; i < endIndex; ++i) {
            if (inputFilter[i] <= threshold) {
                selection[k++] = inputData[i];
            }
        }
        return k;
    }

    inline int processMicroBatch(int endIndex, const T2 *inputData, const T1 *inputFilter, T2 *selection,
                                 T1 threshold) final {
        return processMicroBatch(endIndex, inputData, inputFilter, selection, threshold, 0);
    }

};

template<typename T1, typename T2>
class SelectValuesPredication : public SelectValuesImplementation<T1, T2> {
public:
    inline int processMicroBatch(int endIndex, const T2 *inputData, const T1 *inputFilter, T2 *selection,
                                 T1 threshold, int startIndex) final {
        auto k = 0;
        for (auto i = startIndex; i < endIndex; ++i) {
            selection[k] = inputData[i];
//        selection[k] = inputData[(0L - (inputFilter[i] <= threshold)) & i];   // Double predication
            k += (inputFilter[i] <= threshold);
        }
        return k;
    }

    inline int processMicroBatch(int endIndex, const T2 *inputData, const T1 *inputFilter, T2 *selection,
                                 T1 threshold) final {
        return processMicroBatch(endIndex, inputData, inputFilter, selection, threshold, 0);
    }

};

template<typename T1, typename T2>
struct SelectValuesVectorizedStruct {
    static int run(int endIndex, const T2 *inputData, const T1 *inputFilter,
                   T2 *selection, T1 threshold, int startIndex) {
        std::cerr << "Non-specialised template for SelectValuesVectorized called. Ensure that a specialised template "
                     "has been implemented for the inputFilter data type requested" << std::endl;
        exit(1);
    }
};

#ifdef __AVX512F__

template<typename T>
struct SelectValuesVectorizedStruct<int, T> {
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
struct SelectValuesVectorizedStruct<int64_t, T> {
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
struct SelectValuesVectorizedStruct<float, T> {
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

#else
#ifdef __AVX2__

template<typename T>
struct SelectValuesVectorizedStruct<int, T> {
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
            __m256i mask = ~_mm256_cmpgt_epi32(filterVector, thresholdVector);

            // Convert the mask to a 32-bit integer mask
            unsigned int maskInt = static_cast<unsigned int>(_mm256_movemask_ps(_mm256_castsi256_ps(mask)));

            for (auto j = 0; j < simdWidth; ++j) {
                selection[k] = inputData[i + j];
                k += (maskInt >> j) & 1;
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
struct SelectValuesVectorizedStruct<int64_t, T> {
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
            __m256i mask = ~_mm256_cmpgt_epi64(filterVector, thresholdVector);

            // Convert the mask to a 32-bit integer mask
            unsigned int maskInt = static_cast<unsigned int>(_mm256_movemask_pd(_mm256_castsi256_pd(mask)));

            for (auto j = 0; j < simdWidth; ++j) {
                selection[k] = inputData[i + j];
                k += (maskInt >> j) & 1;
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
struct SelectValuesVectorizedStruct<float, T> {
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
            __m256 mask = _mm256_cmp_ps(filterVector, thresholdVector, _CMP_LE_OQ);

            // Convert the mask to a 32-bit integer mask
            unsigned int maskInt = static_cast<unsigned int>(_mm256_movemask_ps(mask));

            for (auto j = 0; j < simdWidth; ++j) {
                selection[k] = inputData[i + j];
                k += (maskInt >> j) & 1;
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

#else // AVX

template<typename T>
struct SelectValuesVectorizedStruct<int, T> {
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
struct SelectValuesVectorizedStruct<int64_t, T> {
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

#endif
#endif

template<typename T1, typename T2>
class SelectValuesVectorized : public SelectValuesImplementation<T1, T2> {
public:
    inline int processMicroBatch(int endIndex, const T2 *inputData, const T1 *inputFilter, T2 *selection,
                                 T1 threshold, int startIndex) final {
        return SelectValuesVectorizedStruct<T1,T2>::run(endIndex, inputData, inputFilter, selection, threshold,
                                                        startIndex);
    }

    inline int processMicroBatch(int endIndex, const T2 *inputData, const T1 *inputFilter, T2 *selection,
                                 T1 threshold) final {
        return processMicroBatch(endIndex, inputData, inputFilter, selection, threshold, 0);
    }

};

/****************************** SINGLE-THREADED ******************************/

template<typename T1, typename T2>
class MonitorSelectValues;

template<typename T1, typename T2>
class SelectValuesAdaptive {
public:
    SelectValuesAdaptive(int n_, const T2 *inputData_, const T1 *inputFilter_, T2 *selection_, T1 threshold_);
    void adjustRobustness(int adjustment);
    int processInput();

private:
    inline void processMicroBatch();

    int remainingTuples;
    const T2 *inputData;
    const T1 *inputFilter;
    T2 *selection;
    T1 threshold;

    int tuplesPerHazardCheck;
    int maxConsecutiveVectorized;
    int tuplesInBranchBurst;

    int microBatchStartIndex;
    int totalSelected;
    int consecutiveVectorized;
    SelectValuesOperators activeOperator;

    MonitorSelectValues<T1,T2> monitor;
    SelectValuesBranch<T1,T2> branchOperator;
    SelectValuesVectorized<T1,T2> vectorizedOperator;

    int microBatchSize{};
    int microBatchSelected{};
};

template<typename T1, typename T2>
class MonitorSelectValues {
public:
    explicit MonitorSelectValues(SelectValuesAdaptive<T1,T2> *selectOperator_) : selectOperator(selectOperator_) {
        std::vector<std::string> counters = {"PERF_COUNT_HW_BRANCH_MISSES"};
        branchMispredictions = Counters::getInstance().getSharedEventSetEvents(counters);

        std::string mispredictionsName = "SelectValuesLowerMispredictions_" + std::to_string(sizeof(T1)) +
                                         "B_inputFilter_" + std::to_string(sizeof(T2)) + "B_inputData_1_dop";
        std::string selectivityName = "SelectValuesLowerSelectivity_" + std::to_string(sizeof(T1)) +
                                      "B_inputFilter_" + std::to_string(sizeof(T2)) + "B_inputData_1_dop";
        branchCrossoverSelectivity = static_cast<float>(MachineConstants::getInstance().
                getMachineConstant(mispredictionsName));
        vectorizationCrossoverSelectivity = static_cast<float>(MachineConstants::getInstance().
                getMachineConstant(selectivityName));
    }

    void checkHazards(int n, int selected) {
        float selectivity = static_cast<float>(selected) / static_cast<float>(n);

        if (__builtin_expect(((static_cast<float>(*branchMispredictions) / static_cast<float>(n)) >
                            branchCrossoverSelectivity) || (selectivity > vectorizationCrossoverSelectivity), false)) {
            selectOperator->adjustRobustness(1);
        }

        if (__builtin_expect((selectivity < branchCrossoverSelectivity), false)) {
            selectOperator->adjustRobustness(-1);
        }
    }

private:
    const long_long *branchMispredictions;
    float branchCrossoverSelectivity;
    float vectorizationCrossoverSelectivity;
    SelectValuesAdaptive<T1,T2> *selectOperator;
};

template<typename T1, typename T2>
SelectValuesAdaptive<T1,T2>::SelectValuesAdaptive(int n_, const T2 *inputData_, const T1 *inputFilter_, T2 *selection_,
                                                  T1 threshold_) :
        remainingTuples(n_),
        inputData(inputData_),
        inputFilter(inputFilter_),
        selection(selection_),
        threshold(threshold_),
        tuplesPerHazardCheck(50000),
        maxConsecutiveVectorized(10),
        tuplesInBranchBurst(1000),
        microBatchStartIndex(0),
        totalSelected(0),
        consecutiveVectorized(0),
        activeOperator(SelectValuesOperators::ValuesBranch),
        monitor(MonitorSelectValues<T1,T2>(this)),
        branchOperator(SelectValuesBranch<T1,T2>()),
        vectorizedOperator(SelectValuesVectorized<T1,T2>()) {}

template<typename T1, typename T2>
void SelectValuesAdaptive<T1,T2>::adjustRobustness(int adjustment) {
    if (__builtin_expect((adjustment > 0) &&
                         activeOperator == SelectValuesOperators::ValuesBranch, false)) {
//            std::cout << "Switched to select vectorized" << std::endl;
        activeOperator = SelectValuesOperators::ValuesVectorized;
    } else if (__builtin_expect((adjustment < 0) &&
                                activeOperator == SelectValuesOperators::ValuesVectorized, false)) {
//            std::cout << "Switched to select branch" << std::endl;
        activeOperator = SelectValuesOperators::ValuesBranch;
        consecutiveVectorized = 0;
    }
}

template<typename T1, typename T2>
int SelectValuesAdaptive<T1,T2>::processInput() {
    while (remainingTuples > 0) {
        if (__builtin_expect(consecutiveVectorized == maxConsecutiveVectorized, false)) {
//            std::cout << "Running branch burst" << std::endl;
            activeOperator = SelectValuesOperators::ValuesBranch;
            consecutiveVectorized = 0;
            microBatchSize = std::min(remainingTuples, tuplesInBranchBurst);
        } else {
            microBatchSize = std::min(remainingTuples, tuplesPerHazardCheck);
        }
        processMicroBatch();
        monitor.checkHazards(microBatchSize, microBatchSelected);
    }

    return totalSelected;
}

template<typename T1, typename T2>
inline void SelectValuesAdaptive<T1,T2>::processMicroBatch() {
    microBatchSelected = 0;
    if (activeOperator == SelectValuesOperators::ValuesBranch) {
        Counters::getInstance().readSharedEventSet();
        microBatchSelected = branchOperator.processMicroBatch(microBatchStartIndex + microBatchSize, inputData,
                                                              inputFilter, selection, threshold, microBatchStartIndex);
        Counters::getInstance().readSharedEventSet();
    } else {
        Counters::getInstance().readSharedEventSet();
        microBatchSelected = vectorizedOperator.processMicroBatch(microBatchStartIndex + microBatchSize, inputData,
                                                                  inputFilter, selection, threshold,
                                                                  microBatchStartIndex);
        Counters::getInstance().readSharedEventSet();
        consecutiveVectorized++;
    }
    remainingTuples -= microBatchSize;
    microBatchStartIndex += microBatchSize;
    selection += microBatchSelected;
    totalSelected += microBatchSelected;
}

/****************************** MULTI-THREADED ******************************/

template<typename T1, typename T2>
struct SelectValuesThreadArgs {
    vectorOfPairs<int, int> *taskIndexes;
    std::atomic<int> *tasksCompleted;
    int maxSize;
    const T2* inputData;
    const T1* inputFilter;
    T2 *selection;
    T1 threshold;
    std::atomic<int> *selectedCount;
    int dop;
};

template<typename T1, typename T2>
class MonitorSelectValuesParallel;

template<typename T1, typename T2>
class SelectValuesAdaptiveParallelAux {
public:
    explicit SelectValuesAdaptiveParallelAux(SelectValuesThreadArgs<T1,T2>* args);
    void adjustRobustness(int adjustment);
    int processInput();
    void mergeOutput();
    ~SelectValuesAdaptiveParallelAux();

private:
    inline void processMicroBatch();

    const T2* inputData;
    const T1 *inputFilter;
    T2 *threadSelection;
    T2 *threadSelectionStart;
    T2 *overallSelection;
    T1 threshold;

    int tuplesPerHazardCheck;
    int maxConsecutiveVectorized;
    int tuplesInBranchBurst;

    SelectValuesOperators activeOperator;

    SelectValuesBranch<T1,T2> branchOperator;
    SelectValuesVectorized<T1,T2> vectorizedOperator;

    vectorOfPairs<int, int> *taskIndexes;
    std::atomic<int> *tasksCompleted;
    std::atomic<int> *selectedCount;

    int dop;
    long_long *branchMispredictions;
    MonitorSelectValuesParallel<T1,T2> *monitor;
    int eventSet;

    int microBatchStartIndex{};
    int threadSelected{};
    int consecutiveVectorized{};
    int microBatchSize{};
    int microBatchSelected{};
    int taskSelected{};
    int taskTuplesRemaining{};
};

template<typename T1, typename T2>
class MonitorSelectValuesParallel {
public:
    MonitorSelectValuesParallel(SelectValuesAdaptiveParallelAux<T1,T2> *selectOperator_, int dop,
                                 long_long *branchMispredictions_) :
            branchMispredictions(branchMispredictions_),
            selectOperator(selectOperator_) {

        std::string mispredictionsName = "SelectValuesLowerMispredictions_" + std::to_string(sizeof(T1)) +
                                         "B_inputFilter_" + std::to_string(sizeof(T2)) + "B_inputData_1_dop";
        std::string selectivityName = "SelectValuesLowerSelectivity_" + std::to_string(sizeof(T1)) +
                                      "B_inputFilter_" + std::to_string(sizeof(T2)) + "B_inputData_1_dop";
        branchCrossoverSelectivity = static_cast<float>(MachineConstants::getInstance().
                getMachineConstant(mispredictionsName));
        vectorizationCrossoverSelectivity = static_cast<float>(MachineConstants::getInstance().
                getMachineConstant(selectivityName));
    }

    void checkHazards(int n, int selected) {
        float selectivity = static_cast<float>(selected) / static_cast<float>(n);

        if (__builtin_expect(((static_cast<float>(*branchMispredictions) / static_cast<float>(n)) >
                              branchCrossoverSelectivity) || (selectivity > vectorizationCrossoverSelectivity), false)) {
            selectOperator->adjustRobustness(1);
        }

        if (__builtin_expect((selectivity < branchCrossoverSelectivity), false)) {
            selectOperator->adjustRobustness(-1);
        }
    }

private:
    long_long *branchMispredictions;
    float branchCrossoverSelectivity;
    float vectorizationCrossoverSelectivity;
    SelectValuesAdaptiveParallelAux<T1,T2> *selectOperator;
};

template<typename T1, typename T2>
SelectValuesAdaptiveParallelAux<T1,T2>::SelectValuesAdaptiveParallelAux(SelectValuesThreadArgs<T1,T2>* args) :
        tuplesPerHazardCheck(50000),
        maxConsecutiveVectorized(10),
        tuplesInBranchBurst(1000),
        activeOperator(SelectValuesOperators::ValuesBranch),
        branchOperator(SelectValuesBranch<T1,T2>()),
        vectorizedOperator(SelectValuesVectorized<T1,T2>()) {

    threadSelection = new T2[args->maxSize];
    threadSelectionStart = threadSelection;

    inputData = args->inputData;
    inputFilter = args->inputFilter;
    overallSelection = args->selection;
    threshold = args->threshold;
    dop = args->dop;

    taskIndexes = args->taskIndexes;
    tasksCompleted = args->tasksCompleted;
    selectedCount = args->selectedCount;

    delete args;

    eventSet = PAPI_NULL;
    std::vector<std::string> counters = {"PERF_COUNT_HW_BRANCH_MISSES"};
    createThreadEventSet(&eventSet, counters);

    branchMispredictions = new long_long;
    monitor = new MonitorSelectValuesParallel<T1,T2>(this, dop, branchMispredictions);
}

template<typename T1, typename T2>
void SelectValuesAdaptiveParallelAux<T1,T2>::adjustRobustness(int adjustment) {
    if (__builtin_expect((adjustment > 0) &&
                         activeOperator == SelectValuesOperators::ValuesBranch, false)) {
//            std::cout << "Switched to select predication" << std::endl;
        activeOperator = SelectValuesOperators::ValuesVectorized;
    } else if (__builtin_expect((adjustment < 0) &&
                                activeOperator == SelectValuesOperators::ValuesVectorized, false)) {
//            std::cout << "Switched to select branch" << std::endl;
        activeOperator = SelectValuesOperators::ValuesBranch;
        consecutiveVectorized = 0;
    }
}

template<typename T1, typename T2>
int SelectValuesAdaptiveParallelAux<T1,T2>::processInput() {
    int nextTaskNumber = (*tasksCompleted).fetch_add(1);

    while (nextTaskNumber < static_cast<int>(taskIndexes->size())) {

        microBatchStartIndex = (*taskIndexes)[nextTaskNumber].first;
        taskTuplesRemaining = (*taskIndexes)[nextTaskNumber].second;
        taskSelected = 0;
        consecutiveVectorized = 0;
        activeOperator = SelectValuesOperators::ValuesBranch;

        while (taskTuplesRemaining > 0) {
            if (__builtin_expect(consecutiveVectorized == maxConsecutiveVectorized, false)) {
                activeOperator = SelectValuesOperators::ValuesBranch;
                consecutiveVectorized = 0;
                microBatchSize = std::min(taskTuplesRemaining, tuplesInBranchBurst);

            } else {
                microBatchSize = std::min(taskTuplesRemaining, tuplesPerHazardCheck);

            }
            processMicroBatch();
            monitor->checkHazards(microBatchSize, microBatchSelected);
        }

        threadSelected += taskSelected;
        nextTaskNumber = (*tasksCompleted).fetch_add(1);
    }

    return threadSelected;
}

template<typename T1, typename T2>
inline void SelectValuesAdaptiveParallelAux<T1,T2>::processMicroBatch() {
    microBatchSelected = 0;
    if (activeOperator == SelectValuesOperators::ValuesBranch) {
        readThreadEventSet(eventSet, 1, branchMispredictions);
        microBatchSelected = branchOperator.processMicroBatch(microBatchStartIndex + microBatchSize, inputData,
                                                              inputFilter, threadSelection, threshold,
                                                              microBatchStartIndex);
        readThreadEventSet(eventSet, 1, branchMispredictions);
    } else {
        readThreadEventSet(eventSet, 1, branchMispredictions);
        microBatchSelected = vectorizedOperator.processMicroBatch(microBatchStartIndex + microBatchSize, inputData,
                                                                  inputFilter, threadSelection, threshold,
                                                                  microBatchStartIndex);
        readThreadEventSet(eventSet, 1, branchMispredictions);
        consecutiveVectorized++;
    }
    taskTuplesRemaining -= microBatchSize;
    microBatchStartIndex += microBatchSize;
    threadSelection += microBatchSelected;
    taskSelected += microBatchSelected;
}

template<typename T1, typename T2>
void SelectValuesAdaptiveParallelAux<T1,T2>::mergeOutput() {
    int overallSelectionStartIndex = (*selectedCount).fetch_add(threadSelected);
    memcpy(overallSelection + overallSelectionStartIndex, threadSelectionStart, threadSelected * sizeof(int));
}

template<typename T1, typename T2>
SelectValuesAdaptiveParallelAux<T1,T2>::~SelectValuesAdaptiveParallelAux() {
    delete []threadSelectionStart;
    destroyThreadEventSet(eventSet, branchMispredictions);
    delete branchMispredictions;
    delete monitor;
}

template<typename T1, typename T2>
void *selectValuesAdaptiveParallelAux(void *arg) {
    auto* args = static_cast<SelectValuesThreadArgs<T1,T2>*>(arg);
    SelectValuesAdaptiveParallelAux<T1,T2> selectOperator(args);

    selectOperator.processInput();
    selectOperator.mergeOutput();

    return nullptr;
}

template<typename T1, typename T2>
class SelectValuesAdaptiveParallel {
public:
    SelectValuesAdaptiveParallel(int n_, const T2 *inputData_, const T1 *inputFilter_, T2 *selection_,
                                 T1 threshold_, int dop_) :
            n(n_),
            inputData(inputData_),
            inputFilter(inputFilter_),
            selection(selection_),
            threshold(threshold_),
            dop(dop_) {
        assert(1 < dop && dop <= logicalCoresCount());
        Counters::getInstance();
        MachineConstants::getInstance();
    }

    int processInput() {
        pthread_t threads[dop];

        int adaptivePeriod = 50000;
        int tuplesPerTask = std::max(adaptivePeriod * 20, n / (dop * 20));
        if ((n / tuplesPerTask) < (4 * dop)) {
            tuplesPerTask = n / dop;
        }

        vectorOfPairs<int, int> taskIndexes(n / tuplesPerTask, std::make_pair(0, tuplesPerTask));
        taskIndexes.back().second = n - (((n / tuplesPerTask) - 1) * (tuplesPerTask));

        int startIndex = 0;
        for (auto & taskIndex : taskIndexes) {
            taskIndex.first = startIndex;
            startIndex += taskIndex.second;
        }

        std::atomic<int> selectedCount = 0;
        std::atomic<int> tasksCompleted = 0;

        std::vector<SelectValuesThreadArgs<T1,T2>*> threadArgs(dop);

        for (auto i = 0; i < dop; ++i) {
            threadArgs[i] = new SelectValuesThreadArgs<T1,T2>;
            threadArgs[i]->taskIndexes = &taskIndexes;
            threadArgs[i]->tasksCompleted = &tasksCompleted;
            threadArgs[i]->maxSize = n;
            threadArgs[i]->inputData = inputData;
            threadArgs[i]->inputFilter = inputFilter;
            threadArgs[i]->selection = selection;
            threadArgs[i]->threshold = threshold;
            threadArgs[i]->selectedCount = &selectedCount;
            threadArgs[i]->dop = dop;

            pthread_create(&threads[i], NULL, selectValuesAdaptiveParallelAux<T1,T2>, threadArgs[i]);
        }

        for (int i = 0; i < dop; ++i) {
            pthread_join(threads[i], nullptr);
        }

        return selectedCount;
    }

private:
    int n;
    const T2 *inputData;
    const T1 *inputFilter;
    T2 *selection;
    T1 threshold;
    int dop;
};

/****************************** ENTRY FUNCTIONS ******************************/

template<typename T1, typename T2>
struct runSelectFunctionStruct { static int run (Select selectImplementation,
                                                 int n, const T2 *inputData, const T1 *inputFilter,
                                                 T2 *selection, T1 threshold, int dop) {
        if (selectImplementation == Select::ImplementationValuesBranch) {
            SelectValuesBranch<T1,T2> selectOperator;
            return selectOperator.processMicroBatch(n, inputData, inputFilter, selection, threshold);
        }
        if (selectImplementation == Select::ImplementationValuesPredication) {
            SelectValuesPredication<T1,T2> selectOperator;
            return selectOperator.processMicroBatch(n, inputData, inputFilter, selection, threshold);
        }
        if (selectImplementation == Select::ImplementationValuesVectorized) {
            SelectValuesVectorized<T1,T2> selectOperator;
            return selectOperator.processMicroBatch(n, inputData, inputFilter, selection, threshold);
        }
        if (selectImplementation == Select::ImplementationValuesAdaptive) {
            SelectValuesAdaptive<T1,T2> selectOperator(n, inputData, inputFilter, selection, threshold);
            return selectOperator.processInput();
        }
        if (selectImplementation == Select::ImplementationValuesAdaptiveParallel) {
            SelectValuesAdaptiveParallel<T1,T2> selectOperator(n, inputData, inputFilter, selection,
                                                               threshold, dop);
            return selectOperator.processInput();
        }
        std::cout << "Invalid selection of 'Select' implementation!" << std::endl;
        exit(1);
    }
};

template<typename T>
struct runSelectFunctionStruct<T, int> { static int run (Select selectImplementation,
                                                         int n, const int *inputData, const T *inputFilter,
                                                         int *selection, T threshold, int dop) {
        if (selectImplementation == Select::ImplementationIndexesBranch) {
            SelectIndexesBranch<T> selectOperator;
            return selectOperator.processMicroBatch(n, inputFilter, selection, threshold);
        }
        if (selectImplementation == Select::ImplementationIndexesPredication) {
            SelectIndexesPredication<T> selectOperator;
            return selectOperator.processMicroBatch(n, inputFilter, selection, threshold);
        }
        if (selectImplementation == Select::ImplementationIndexesAdaptive) {
            SelectIndexesAdaptive<T> selectOperator(n, inputFilter, selection, threshold);
            return selectOperator.processInput();
        }
        if (selectImplementation == Select::ImplementationIndexesAdaptiveParallel) {
            SelectIndexesAdaptiveParallel<T> selectOperator(n, inputFilter, selection, threshold, dop);
            return selectOperator.processInput();
        }
        if (selectImplementation == Select::ImplementationValuesBranch) {
            SelectValuesBranch<T,int> selectOperator;
            return selectOperator.processMicroBatch(n, inputData, inputFilter, selection, threshold);
        }
        if (selectImplementation == Select::ImplementationValuesPredication) {
            SelectValuesPredication<T,int> selectOperator;
            return selectOperator.processMicroBatch(n, inputData, inputFilter, selection, threshold);
        }
        if (selectImplementation == Select::ImplementationValuesVectorized) {
            SelectValuesVectorized<T,int> selectOperator;
            return selectOperator.processMicroBatch(n, inputData, inputFilter, selection, threshold);
        }
        if (selectImplementation == Select::ImplementationValuesAdaptive) {
            SelectValuesAdaptive<T,int> selectOperator(n, inputData, inputFilter, selection, threshold);
            return selectOperator.processInput();
        }
        if (selectImplementation == Select::ImplementationValuesAdaptiveParallel) {
            SelectValuesAdaptiveParallel<T,int> selectOperator(n, inputData, inputFilter, selection,
                                                               threshold, dop);
            return selectOperator.processInput();
        }
        std::cout << "Invalid selection of 'Select' implementation!" << std::endl;
        exit(1);
    }
};

template<typename T1, typename T2>
int runSelectFunction(Select selectImplementation,
                      int n, const T2 *inputData, const T1 *inputFilter, T2 *selection, T1 threshold, int dop) {
    return runSelectFunctionStruct<T1,T2>::run(selectImplementation, n, inputData, inputFilter, selection, threshold, dop);
}

}

#endif //HAQP_SELECT_IMPLEMENTATION_H

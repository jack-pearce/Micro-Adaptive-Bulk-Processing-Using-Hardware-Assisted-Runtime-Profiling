#ifndef HAQP_GROUPBYIMPLEMENTATION_H
#define HAQP_GROUPBYIMPLEMENTATION_H


#include <iostream>
#include <limits>
#include <atomic>
#include <condition_variable>
#include <tsl/robin_map.h>

#include "utilities/systemInformation.h"
#include "utilities/papiWrapper.h"
#include "machine_constants/machineConstants.h"


namespace HAQP {

#ifdef __AVX512F__
    constexpr int BITS_PER_GROUPBY_RADIX_PASS = 10;
#else
#ifdef __AVX2__
    constexpr int BITS_PER_GROUPBY_RADIX_PASS = 9;
#else // AVX
    constexpr int BITS_PER_GROUPBY_RADIX_PASS = 8;
#endif
#endif

/****************************** AGGREGATION FUNCTIONS ******************************/

template<typename T>
T MinAggregation<T>::operator()(T currentAggregate, T numberToInclude, bool firstAggregation) const {
    if (firstAggregation) {
        return numberToInclude;
    }
    return std::min(currentAggregate, numberToInclude);
}

template<typename T>
T MaxAggregation<T>::operator()(T currentAggregate, T numberToInclude, bool firstAggregation) const {
    if (firstAggregation) {
        return numberToInclude;
    }
    return std::max(currentAggregate, numberToInclude);
}

template<typename T>
T SumAggregation<T>::operator()(T currentAggregate, T numberToInclude, bool firstAggregation) const {
    if (firstAggregation) {
        return numberToInclude;
    }
    return currentAggregate + numberToInclude;
}

template<typename T>
T CountAggregation<T>::operator()(T currentAggregate, T _, bool firstAggregation) const {
    if (firstAggregation) {
        return 1;
    }
    return ++currentAggregate;
}

/****************************** FOUNDATIONAL ALGORITHMS ******************************/

template<template<typename> class Aggregator, typename T1, typename T2>
class GroupByImplementation {
public:
    virtual inline void processMicroBatch(int n, int startIndex) = 0;
    virtual inline void processMicroBatch(int n) = 0;

    virtual vectorOfPairs<T1, T2> getOutput() = 0;
    virtual ~GroupByImplementation() = default;
};

template<template<typename> class Aggregator, typename T1, typename T2>
class GroupByHash : public GroupByImplementation<Aggregator,T1,T2> {
public:
    GroupByHash() = delete;
    ~GroupByHash() override = default;
    GroupByHash(T1 *inputGroupBy_, T2 *inputAggregate_, int cardinality) :
            inputGroupBy(inputGroupBy_),
            inputAggregate(inputAggregate_),
            map(std::max(static_cast<int>(2.5 * cardinality), 400000)) /* Should be O(1) */ {
        static_assert(std::is_integral<T1>::value, "GroupBy column must be an integer type");
        static_assert(std::is_arithmetic<T2>::value, "Payload column must be an numeric type");
    }

    inline void processMicroBatch(int n, int startIndex) final {
        for (int index = startIndex; index < startIndex + n; ++index) {
            it = map.find(inputGroupBy[index]);
            if (it != map.end()) {
                it.value() = Aggregator<T2>()(it->second, inputAggregate[index], false);
            } else {
                map.insert({inputGroupBy[index], Aggregator<T2>()(0, inputAggregate[index], true)});
            }
        }
    };

    inline void processMicroBatch(int n) final {
        return processMicroBatch(n, 0);
    }

    inline T1 processMicroBatchReturnLargest(int n, int startIndex) {
        T1 largest = std::numeric_limits<T1>::lowest();
        for (int index = startIndex; index < startIndex + n; ++index) {
            it = map.find(inputGroupBy[index]);
            if (it != map.end()) {
                it.value() = Aggregator<T2>()(it->second, inputAggregate[index], false);
            } else {
                map.insert({inputGroupBy[index], Aggregator<T2>()(0, inputAggregate[index], true)});
                largest = std::max(largest, inputGroupBy[index]);
            }
        }
        return largest;
    };

    inline T1 processMicroBatchReturnLargest(int n) {
        return processMicroBatchReturnLargest(n, 0);
    }

    vectorOfPairs<T1, T2> getOutput() final {
        return {map.begin(), map.end()};
    }

    tsl::robin_map<T1, T2> && getState() {
        return std::move(map);
    }

private:
    T1 *inputGroupBy;
    T2 *inputAggregate;

    tsl::robin_map<T1, T2> map;
    typename tsl::robin_map<T1, T2>::iterator it;
};

template<template<typename> class Aggregator, typename T1, typename T2>
class GroupBySort : public GroupByImplementation<Aggregator,T1,T2> {
public:
    GroupBySort() = delete;
    GroupBySort(T1 *inputGroupBy_, T2 *inputAggregate_) :
            inputGroupBy(inputGroupBy_),
            inputAggregate(inputAggregate_),
            totalTuplesToProcess(0),
            numBuckets(1 << BITS_PER_GROUPBY_RADIX_PASS),
            mask(numBuckets - 1),
            largest(std::numeric_limits<T1>::min()),
            bufferGroupBy(nullptr),
            bufferAggregate(nullptr) {
        static_assert(std::is_integral<T1>::value, "GroupBy column must be an integer type");
        static_assert(std::is_arithmetic<T2>::value, "Payload column must be an numeric type");
    }

    void addMap(tsl::robin_map<T1, T2> && map_, T1 mapLargest) noexcept {
        std::cout << "Map added" << std::endl;
        map = map_;
        largest = std::max(largest, mapLargest);
        totalTuplesToProcess += map.size();
    }

    inline void processMicroBatch(int n, int startIndex) final {
        microBatchesToProcess.emplace_back(startIndex, startIndex + n);
        totalTuplesToProcess += n;
    };

    inline void processMicroBatch(int n) final {
        return processMicroBatch(n, 0);
    }

    ~GroupBySort() override {
        if (bufferGroupBy) {
            delete[]bufferGroupBy;
            delete[]bufferAggregate;
        }
    }

private:
    void getOutputAuxAgg(int startIndex, int endIndex, T1* inputGroupByTmp, T2* inputAggregateTmp,
                         vectorOfPairs<T1, T2> &output) {
        int i;
        bool bucketEntryPresent[1 << BITS_PER_GROUPBY_RADIX_PASS] = {false};

        for (i = startIndex; i < endIndex; i++) {
            buckets[inputGroupByTmp[i] & mask] = Aggregator<T2>()(buckets[inputGroupByTmp[i] & mask],
                    inputAggregateTmp[i], !bucketEntryPresent[inputGroupByTmp[i] & mask]);
            bucketEntryPresent[inputGroupByTmp[i] & mask] = true;
        }

        int valuePrefix = inputGroupByTmp[startIndex] & ~mask;

        for (i = 0; i < numBuckets; i++) {
            if (bucketEntryPresent[i]) {
                output.emplace_back(valuePrefix | i, buckets[i]);
            }
        }

        std::fill(buckets.begin(), buckets.end(), 0);
    }

    void getOutputAux(int startIndex, int endIndex, T1* inputGroupByTmp, T2* inputAggregateTmp, T1* bufferGroupByTmp,
                      T2* bufferAggregateTmp, int pass, vectorOfPairs<T1, T2> &output) {
        int i;

        for (i = startIndex; i < endIndex; i++) {
            buckets[(inputGroupByTmp[i] >> (pass * BITS_PER_GROUPBY_RADIX_PASS)) & mask]++;
        }

        for (i = 1; i < numBuckets; i++) {
            buckets[i] += buckets[i - 1];
        }

        std::vector<int> partitions(buckets.data(), buckets.data() + numBuckets);
        for (i = 0; i < numBuckets; i++) {
            partitions[i] += startIndex;
        }

        for (i = endIndex - 1; i >= startIndex; i--) {
            bufferGroupByTmp[startIndex + --buckets[(inputGroupByTmp[i] >>
                (pass * BITS_PER_GROUPBY_RADIX_PASS)) & mask]] = inputGroupByTmp[i];
            bufferAggregateTmp[startIndex + buckets[(inputGroupByTmp[i] >>
                (pass * BITS_PER_GROUPBY_RADIX_PASS)) & mask]] = inputAggregateTmp[i];
        }

        std::fill(buckets.begin(), buckets.end(), 0);
        std::swap(inputGroupByTmp, bufferGroupByTmp);
        std::swap(inputAggregateTmp, bufferAggregateTmp);
        --pass;

        int previousStart = startIndex;
        if (pass > 0) {
            for (i = 0; i < numBuckets; i++) {
                if (partitions[i] > previousStart) {
                    getOutputAux(previousStart, partitions[i], inputGroupByTmp, inputAggregateTmp, bufferGroupByTmp,
                                 bufferAggregateTmp, pass, output);
                }
                previousStart = partitions[i];
            }
        } else {
            for (i = 0; i < numBuckets; i++) {
                if (partitions[i] > previousStart) {
                    getOutputAuxAgg(previousStart, partitions[i], inputGroupByTmp, inputAggregateTmp, output);
                }
                previousStart = partitions[i];
            }
        }
    }

    T1 *inputGroupBy;
    T2 *inputAggregate;

    tsl::robin_map<T1, T2> map;
    vectorOfPairs<int, int> microBatchesToProcess;
    int totalTuplesToProcess;

    int numBuckets;
    int mask;
    T1 largest;

    std::vector<int> buckets;
    T1 *bufferGroupBy;
    T2 *bufferAggregate;

public:
    vectorOfPairs<T1, T2> getOutput(vectorOfPairs<T1, T2> &output) {
        int i;
        for (const auto& microBatch : microBatchesToProcess) {
            for (i = microBatch.first; i < microBatch.second; i++) {
                largest = std::max(largest, inputGroupBy[i]);
            }
        }

        int msbPosition = 0;
        while (largest != 0) {
            largest >>= 1;
            msbPosition++;
        }

        int pass = static_cast<int>(std::ceil(static_cast<double>(msbPosition) / BITS_PER_GROUPBY_RADIX_PASS)) - 1;

        buckets = std::vector<int>(1 << BITS_PER_GROUPBY_RADIX_PASS, 0);
        bufferGroupBy = new T1[totalTuplesToProcess];
        bufferAggregate = new T2[totalTuplesToProcess];

        for (const auto& microBatch : microBatchesToProcess) {
            for (i = microBatch.first; i < microBatch.second; i++) {
                buckets[(inputGroupBy[i] >> (pass * BITS_PER_GROUPBY_RADIX_PASS)) & mask]++;
            }
        }
        for (auto it = map.begin(); it != map.end(); ++it) {
            buckets[(it->first >> (pass * BITS_PER_GROUPBY_RADIX_PASS)) & mask]++;
        }

        for (i = 1; i < numBuckets; i++) {
            buckets[i] += buckets[i - 1];
        }

        std::vector<int> partitions(buckets.data(), buckets.data() + numBuckets);

        for (auto it = map.begin(); it != map.end(); it++) {
            bufferGroupBy[--buckets[(it->first >> (pass * BITS_PER_GROUPBY_RADIX_PASS)) & mask]] = it->first;
            bufferAggregate[buckets[(it->first >> (pass * BITS_PER_GROUPBY_RADIX_PASS)) & mask]] = it->second;
        }
        for (const auto& microBatch :
                vectorOfPairs<int, int>(microBatchesToProcess.rbegin(), microBatchesToProcess.rend())) {
            for (i = microBatch.first; i < microBatch.second; i++) {
                bufferGroupBy[--buckets[(inputGroupBy[i] >> (pass * BITS_PER_GROUPBY_RADIX_PASS)) & mask]] =
                        inputGroupBy[i];
                bufferAggregate[buckets[(inputGroupBy[i] >> (pass * BITS_PER_GROUPBY_RADIX_PASS)) & mask]] =
                        inputAggregate[i];
            }
        }

        std::fill(buckets.begin(), buckets.end(), 0);
        std::swap(inputGroupBy, bufferGroupBy);
        std::swap(inputAggregate, bufferAggregate);
        --pass;

        int previousStart = 0;
        if (pass > 0) {
            for (i = 0; i < numBuckets; i++) {
                if (partitions[i] > previousStart) {
                    getOutputAux(previousStart, partitions[i], inputGroupBy, inputAggregate, bufferGroupBy,
                                 bufferAggregate, pass, output);
                }
                previousStart = partitions[i];
            }
        } else {
            for (i = 0; i < numBuckets; i++) {
                if (partitions[i] > previousStart) {
                    getOutputAuxAgg(previousStart, partitions[i], inputGroupBy, inputAggregate, output);
                }
                previousStart = partitions[i];
            }
        }

        std::swap(inputGroupBy, bufferGroupBy);
        std::swap(inputAggregate, bufferAggregate);

        return output;
    }

    vectorOfPairs<T1, T2> getOutput() final {
        vectorOfPairs<T1, T2> output;
        return getOutput(output);
    }
};

/****************************** SINGLE-THREADED ******************************/

template<template<typename> class Aggregator, typename T1, typename T2>
class MonitorGroupBy;

template<template<typename> class Aggregator, typename T1, typename T2>
class GroupByAdaptive {
public:
    GroupByAdaptive(int n, T1 *inputGroupBy_, T2 *inputAggregate_, int cardinality);
    void adjustRobustness(int adjustment);
    vectorOfPairs<T1, T2> processInput();

private:
    inline void processMicroBatch();

    int remainingTuples;
    int tuplesPerHazardCheck;
    int tuplesBetweenHashTests;

    int microBatchStartIndex;
    T1 mapLargest;
    GroupBy activeOperator;
    bool sortOperatorUsed;

    MonitorGroupBy<Aggregator, T1, T2> monitor;
    GroupByHash<Aggregator,T1,T2> hashOperator;
    GroupBySort<Aggregator,T1,T2> sortOperator;

    int microBatchSize{};
};

template<template<typename> class Aggregator, typename T1, typename T2>
class MonitorGroupBy {
public:
    explicit MonitorGroupBy(GroupByAdaptive<Aggregator,T1,T2> *groupByOperator_) : groupByOperator(groupByOperator_) {
        std::vector<std::string> counters = {"PERF_COUNT_HW_CACHE_MISSES"};
        lastLevelCacheMisses = Counters::getInstance().getSharedEventSetEvents(counters);

        std::string constantName = "GroupBy_" + std::to_string(sizeof(T1)) + "B_inputFilter_" +
                                   std::to_string(sizeof(T2)) + "B_inputAggregate_1_dop";
        tuplesPerLastLevelCacheMissThreshold = static_cast<float>(MachineConstants::getInstance().
                getMachineConstant(constantName));
    }

    void checkHazards(int tuplesProcessed) {
        if ((static_cast<float>(tuplesProcessed) / *lastLevelCacheMisses) < tuplesPerLastLevelCacheMissThreshold) {
            groupByOperator->adjustRobustness(1);
        }
    }

private:
    const long_long *lastLevelCacheMisses;
    float tuplesPerLastLevelCacheMissThreshold;
    GroupByAdaptive<Aggregator,T1,T2> *groupByOperator;
};

template<template<typename> class Aggregator, typename T1, typename T2>
GroupByAdaptive<Aggregator, T1, T2>::GroupByAdaptive(int n, T1 *inputGroupBy_, T2 *inputAggregate_, int cardinality) :
        remainingTuples(n),
        tuplesPerHazardCheck(75*1000),
        tuplesBetweenHashTests(2 * 1000 * 1000),
        microBatchStartIndex(0),
        mapLargest(std::numeric_limits<T1>::min()),
        activeOperator(GroupBy::Hash),
        sortOperatorUsed(false),
        monitor(MonitorGroupBy<Aggregator, T1, T2>(this)),
        hashOperator(GroupByHash<Aggregator,T1,T2>(inputGroupBy_, inputAggregate_, cardinality)),
        sortOperator(GroupBySort<Aggregator,T1,T2>(inputGroupBy_, inputAggregate_)) {}

template<template<typename> class Aggregator, typename T1, typename T2>
void GroupByAdaptive<Aggregator, T1, T2>::adjustRobustness(int adjustment) {
    if (__builtin_expect((adjustment > 0) && activeOperator == GroupBy::Hash, false)) {
//            std::cout << "Switched to sort at index " << microBatchStartIndex << std::endl;
        activeOperator = GroupBy::Sort;
    } else if (__builtin_expect((adjustment < 0) && activeOperator == GroupBy::Hash, false)) {
//            std::cout << "Switched to hash at index " << microBatchStartIndex << std::endl;
        activeOperator = GroupBy::Hash;
    }
}

template<template<typename> class Aggregator, typename T1, typename T2>
vectorOfPairs<T1, T2> GroupByAdaptive<Aggregator, T1, T2>::processInput() {
    while (remainingTuples > 0) {
        processMicroBatch();
    }

    if (!sortOperatorUsed) {
        return hashOperator.getOutput();
    }

    sortOperator.addMap(hashOperator.getState(), mapLargest);
    return sortOperator.getOutput();
}

template<template<typename> class Aggregator, typename T1, typename T2>
inline void GroupByAdaptive<Aggregator, T1, T2>::processMicroBatch() {
    if (activeOperator == GroupBy::Hash) {
        microBatchSize = std::min(remainingTuples, tuplesPerHazardCheck);
        Counters::getInstance().readSharedEventSet();
        mapLargest = std::max(mapLargest, hashOperator.processMicroBatchReturnLargest(microBatchSize, microBatchStartIndex));
        Counters::getInstance().readSharedEventSet();
        monitor.checkHazards(microBatchSize);
    } else {
        microBatchSize = std::min(remainingTuples, tuplesBetweenHashTests);
        sortOperator.processMicroBatch(microBatchSize, microBatchStartIndex);
        sortOperatorUsed = true;
        activeOperator = GroupBy::Hash;
    }
    remainingTuples -= microBatchSize;
    microBatchStartIndex += microBatchSize;
}

/****************************** MULTI-THREADED ******************************/

template<typename T1, typename T2>
struct GroupByThreadArgs {
    int n;
    T1* inputGroupBy;
    T2* inputAggregate;
    int cardinality;
    vectorOfPairs<T1, T2>* output;
    int threadNumber;
    std::atomic<int> *numFinishedThreads;
    std::vector<bool> *threadFinishedAndWaitingFlags;
    std::condition_variable *cv;
    GroupByThreadArgs *mergeThread1;
    GroupByThreadArgs *mergeThread2;
    int dop;

    ~GroupByThreadArgs() {
        delete output;
    }

    vectorOfPairs<T1, T2> && extractResult() {
        return std::move(*output);
    }
};

template<template<typename> class Aggregator, typename T1, typename T2>
class MonitorGroupByParallel;

template<template<typename> class Aggregator, typename T1, typename T2>
class GroupByAdaptiveParallelAux {
public:
    explicit GroupByAdaptiveParallelAux(GroupByThreadArgs<T1,T2>* args);
    void adjustRobustness(int adjustment);
    void processInput();
    void mergeOutput();
    ~GroupByAdaptiveParallelAux();

private:
    inline void processMicroBatch();

    int remainingTuples;
    int tuplesPerHazardCheck;
    int tuplesBetweenHashTests;

    int microBatchStartIndex;
    T1 mapLargest;
    GroupBy activeOperator;
    bool sortOperatorUsed;

    GroupByThreadArgs<T1,T2>* args;

    MonitorGroupByParallel<Aggregator, T1, T2> *monitor;
    GroupByHash<Aggregator,T1,T2> *hashOperator;
    GroupBySort<Aggregator,T1,T2> *sortOperator;

    int eventSet;
    long_long *lastLevelCacheMisses;

    int microBatchSize {};
};

template<template<typename> class Aggregator, typename T1, typename T2>
class MonitorGroupByParallel {
public:
    explicit MonitorGroupByParallel(GroupByAdaptiveParallelAux<Aggregator,T1,T2> *groupByOperator_, int dop,
                                    long_long *lastLevelCacheMisses_) :
            lastLevelCacheMisses(lastLevelCacheMisses_),
            groupByOperator(groupByOperator_) {

        std::string constantName = "GroupBy_" + std::to_string(sizeof(T1)) + "B_inputFilter_" +
                                   std::to_string(sizeof(T2)) + "B_inputAggregate_" + std::to_string(dop) + "_dop";
        tuplesPerLastLevelCacheMissThreshold = static_cast<float>(MachineConstants::getInstance().
                getMachineConstant(constantName));
    }

    void checkHazards(int tuplesProcessed) {
        if ((static_cast<float>(tuplesProcessed) / *lastLevelCacheMisses) < tuplesPerLastLevelCacheMissThreshold) {
            groupByOperator->adjustRobustness(1);
        }
    }

private:
    long_long *lastLevelCacheMisses;
    float tuplesPerLastLevelCacheMissThreshold;
    GroupByAdaptiveParallelAux<Aggregator,T1,T2> *groupByOperator;
};

template<template<typename> class Aggregator, typename T1, typename T2>
GroupByAdaptiveParallelAux<Aggregator, T1, T2>::GroupByAdaptiveParallelAux(GroupByThreadArgs<T1,T2>* args_) :
        tuplesPerHazardCheck(75*1000),
        tuplesBetweenHashTests(2 * 1000 * 1000),
        microBatchStartIndex(0),
        mapLargest(std::numeric_limits<T1>::min()),
        activeOperator(GroupBy::Hash),
        sortOperatorUsed(false){
    args = static_cast<GroupByThreadArgs<T1, T2>*>(args_);
    remainingTuples = args->n;
    int maxCardinality = std::min(args->cardinality, remainingTuples);

    eventSet = PAPI_NULL;
    std::vector<std::string> counters = {"PERF_COUNT_HW_CACHE_MISSES"};
    createThreadEventSet(&eventSet, counters);

    lastLevelCacheMisses = new long_long;
    monitor = new MonitorGroupByParallel<Aggregator, T1, T2>(this, args->dop, lastLevelCacheMisses);
    hashOperator = new GroupByHash<Aggregator,T1,T2>(args->inputGroupBy, args->inputAggregate, maxCardinality);
    sortOperator = new GroupBySort<Aggregator,T1,T2>(args->inputGroupBy, args->inputAggregate);
}

template<template<typename> class Aggregator, typename T1, typename T2>
void GroupByAdaptiveParallelAux<Aggregator, T1, T2>::adjustRobustness(int adjustment) {
    if (__builtin_expect((adjustment > 0) && activeOperator == GroupBy::Hash, false)) {
//        std::cout << "Switched to sort at index " << microBatchStartIndex << std::endl;
        activeOperator = GroupBy::Sort;
    } else if (__builtin_expect((adjustment < 0) && activeOperator == GroupBy::Hash, false)) {
//            std::cout << "Switched to hash at index " << microBatchStartIndex << std::endl;
        activeOperator = GroupBy::Hash;
    }
}

template<template<typename> class Aggregator, typename T1, typename T2>
void GroupByAdaptiveParallelAux<Aggregator, T1, T2>::processInput() {
    while (remainingTuples > 0) {
        processMicroBatch();
    }

    if (!sortOperatorUsed) {
        tsl::robin_map<T1, T2> map = hashOperator->getState();
        args->output->reserve(map.size());
        args->output->insert(args->output->end(), map.begin(), map.end());
        sortVectorOfPairs(args->output);
        return;
    }

    sortOperator->addMap(hashOperator->getState(), mapLargest);
    sortOperator->getOutput(*(args->output));
}

template<template<typename> class Aggregator, typename T1, typename T2>
inline void GroupByAdaptiveParallelAux<Aggregator, T1, T2>::processMicroBatch() {
    if (activeOperator == GroupBy::Hash) {
        microBatchSize = std::min(remainingTuples, tuplesPerHazardCheck);
        readThreadEventSet(eventSet, 1, lastLevelCacheMisses);
        mapLargest = std::max(mapLargest, hashOperator->processMicroBatchReturnLargest(microBatchSize,
                                                                                       microBatchStartIndex));
        readThreadEventSet(eventSet, 1, lastLevelCacheMisses);
        monitor->checkHazards(microBatchSize);
    } else {
        microBatchSize = std::min(remainingTuples, tuplesBetweenHashTests);
        sortOperator->processMicroBatch(microBatchSize, microBatchStartIndex);
        sortOperatorUsed = true;
        activeOperator = GroupBy::Hash;
    }
    remainingTuples -= microBatchSize;
    microBatchStartIndex += microBatchSize;
}

template<template<typename> class Aggregator, typename T1, typename T2>
void GroupByAdaptiveParallelAux<Aggregator, T1, T2>::mergeOutput() {
    (*(args->threadFinishedAndWaitingFlags))[args->threadNumber] = true;
    (*(args->numFinishedThreads))++;
    args->cv->notify_one();
}

template<template<typename> class Aggregator, typename T1, typename T2>
GroupByAdaptiveParallelAux<Aggregator, T1, T2>::~GroupByAdaptiveParallelAux() {
    destroyThreadEventSet(eventSet, lastLevelCacheMisses);
    delete lastLevelCacheMisses;
    delete monitor;
    delete hashOperator;
    delete sortOperator;
}

template<template<typename> class Aggregator, typename T1, typename T2>
void *groupByAdaptiveParallelAux(void *arg) {
    auto* args = static_cast<GroupByThreadArgs<T1, T2>*>(arg);
    GroupByAdaptiveParallelAux<Aggregator,T1,T2> groupByOperator(args);

    groupByOperator.processInput();
    groupByOperator.mergeOutput();

    return nullptr;
}

template<template<typename> class Aggregator, typename T1, typename T2>
void *groupByAdaptiveParallelPerformMerge(void *arg) {
    auto* args = static_cast<GroupByThreadArgs<T1, T2>*>(arg);
    std::vector<bool> *threadFinishedAndWaitingFlags = args->threadFinishedAndWaitingFlags;
    std::atomic<int> *numFinishedThreads = args->numFinishedThreads;
    std::condition_variable *cv = args->cv;
    GroupByThreadArgs<T1, T2> *mergeThread1 = args->mergeThread1;
    GroupByThreadArgs<T1, T2> *mergeThread2 = args->mergeThread2;

    vectorOfPairs<T1,T2> *inputResult1 = mergeThread1->output;
    vectorOfPairs<T1,T2> *inputResult2 = mergeThread2->output;
    vectorOfPairs<T1,T2> *result = args->output;

    size_t l = 0;
    size_t r = 0;
    while (l < inputResult1->size() && r < inputResult2->size()) {
        if ((*inputResult1)[l].first < (*inputResult2)[r].first) {
            if (!result->empty() && result->back().first == (*inputResult1)[l].first) {
                result->back().second = Aggregator<T2>()(result->back().second, (*inputResult1)[l].second, false);
            } else {
                result->emplace_back((*inputResult1)[l].first, (*inputResult1)[l].second);
            }
            l += 1;
        } else {
            if (!result->empty() && result->back().first == (*inputResult2)[r].first) {
                result->back().second = Aggregator<T2>()(result->back().second, (*inputResult2)[r].second, false);
            } else {
                result->emplace_back((*inputResult2)[r].first, (*inputResult2)[r].second);
            }
            r += 1;
        }
    }

    if (l < inputResult1->size()) {
        if (!result->empty() && result->back().first == (*inputResult1)[l].first) {
            result->back().second = Aggregator<T2>()(result->back().second, (*inputResult1)[l].second, false);
            l += 1;
        }
        result->insert(result->end(), std::make_move_iterator(inputResult1->begin() + l),
                       std::make_move_iterator(inputResult1->end()));
    } else if (r < inputResult2->size()) {
        if (!result->empty() && result->back().first == (*inputResult2)[r].first) {
            result->back().second = Aggregator<T2>()(result->back().second, (*inputResult2)[r].second, false);
            r += 1;
        }
        result->insert(result->end(), std::make_move_iterator(inputResult2->begin() + r),
                       std::make_move_iterator(inputResult2->end()));
    }

    delete mergeThread1;
    delete mergeThread2;

    (*threadFinishedAndWaitingFlags)[args->threadNumber] = true;
    (*numFinishedThreads)++;
    cv->notify_one();
    return nullptr;
}

template<template<typename> class Aggregator, typename T1, typename T2>
class GroupByAdaptiveParallel {
public:
    GroupByAdaptiveParallel(int n_, T1 *inputGroupBy_, T2 *inputAggregate_, int cardinality_, int dop_) :
            n(n_),
            inputGroupBy(inputGroupBy_),
            inputAggregate(inputAggregate_),
            cardinality(cardinality_),
            dop(dop_) {
        static_assert(std::is_integral<T1>::value, "GroupBy column must be an integer type");
        static_assert(std::is_arithmetic<T2>::value, "Payload column must be an numeric type");
        assert(1 < dop && dop <= logicalCoresCount());
        Counters::getInstance();
        MachineConstants::getInstance();

    }

    vectorOfPairs<T1, T2> processInput() {
        int totalThreads = (dop * 2) - 1;
        pthread_t threads[totalThreads];

        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

        std::vector<int> elementsPerThread(dop, n / dop);
        elementsPerThread[dop - 1] = n - ((dop - 1) * (n / dop));
        T1 *threadInputGroupBy = inputGroupBy;
        T2 *threadInputAggregate = inputAggregate;

        std::atomic<int> numFinishedThreads = 0;
        std::vector<bool> threadFinishedAndWaitingFlags(totalThreads, false);
        std::condition_variable cv;
        std::mutex cvMutex;

        std::vector<GroupByThreadArgs<T1, T2>*> threadArgs(totalThreads);

        for (int i = 0; i < dop; ++i) {
            threadArgs[i] = new GroupByThreadArgs<T1, T2>;
            threadArgs[i]->n = elementsPerThread[i];
            threadArgs[i]->inputGroupBy = threadInputGroupBy;
            threadArgs[i]->inputAggregate = threadInputAggregate;
            threadArgs[i]->cardinality = cardinality;
            threadArgs[i]->output = new vectorOfPairs<T1, T2>;
            threadArgs[i]->threadNumber = i;
            threadArgs[i]->numFinishedThreads = &numFinishedThreads;
            threadArgs[i]->threadFinishedAndWaitingFlags = &threadFinishedAndWaitingFlags;
            threadArgs[i]->cv = &cv;
            threadArgs[i]->dop = dop;

            pthread_create(&threads[i], &attr, groupByAdaptiveParallelAux<Aggregator, T1, T2>,
                           threadArgs[i]);

            threadInputGroupBy += elementsPerThread[i];
            threadInputAggregate += elementsPerThread[i];
        }

        pthread_attr_destroy(&attr);
        return mergeOutput(cv, cvMutex, numFinishedThreads, threadFinishedAndWaitingFlags, threadArgs, threads);
    }

    vectorOfPairs<T1, T2> mergeOutput(std::condition_variable &cv,
                                      std::mutex &cvMutex,
                                      std::atomic<int> &numFinishedThreads,
                                      std::vector<bool> &threadFinishedAndWaitingFlags,
                                      std::vector<GroupByThreadArgs<T1, T2>*> &threadArgs,
                                      pthread_t *threads) {
        int mergesComplete = 0;
        std::unique_lock<std::mutex> lock(cvMutex);

        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

        while (mergesComplete < dop - 1) {
            cv.wait(lock, [&numFinishedThreads] { return numFinishedThreads >= 2; });
            numFinishedThreads -= 2;

            int thread1Id = -1;
            int thread2Id = -1;

            for (int i = 0; i < static_cast<int>(threadFinishedAndWaitingFlags.size()); i++) {
                if (threadFinishedAndWaitingFlags[i]) {
                    threadFinishedAndWaitingFlags[i] = false;
                    if (thread1Id < 0) {
                        thread1Id = i;
                    } else {
                        thread2Id = i;
                        break;
                    }
                }
            }

            int threadNumber = dop + mergesComplete;
            threadArgs[threadNumber] = new GroupByThreadArgs<T1, T2>;
            threadArgs[threadNumber]->output = new vectorOfPairs<T1, T2>;
            threadArgs[threadNumber]->threadNumber = threadNumber;
            threadArgs[threadNumber]->numFinishedThreads = &numFinishedThreads;
            threadArgs[threadNumber]->threadFinishedAndWaitingFlags = &threadFinishedAndWaitingFlags;
            threadArgs[threadNumber]->cv = &cv;
            threadArgs[threadNumber]->mergeThread1 = threadArgs[thread1Id];
            threadArgs[threadNumber]->mergeThread2 = threadArgs[thread2Id];

            pthread_create(&threads[threadNumber], &attr,
                           groupByAdaptiveParallelPerformMerge<Aggregator, T1, T2>,threadArgs[threadNumber]);

            mergesComplete++;
        }

        pthread_attr_destroy(&attr);
        cv.wait(lock, [&numFinishedThreads] { return numFinishedThreads == 1; });
        vectorOfPairs<T1, T2> result = threadArgs.back()->extractResult();
        delete threadArgs.back();
        return result;
    }

private:
    int n;
    T1 *inputGroupBy;
    T2 *inputAggregate;
    int cardinality;
    int dop;
};

template<template<typename> class Aggregator, typename T1, typename T2>
vectorOfPairs<T1, T2> runGroupByFunction(GroupBy groupByImplementation, int n, T1 *inputGroupBy, T2 *inputAggregate,
                                         int cardinality, int dop) {
    if (groupByImplementation == GroupBy::Hash) {
        GroupByHash<Aggregator,T1,T2> groupByOperator(inputGroupBy, inputAggregate, cardinality);
        groupByOperator.processMicroBatch(n);
        return groupByOperator.getOutput();
    }
    if (groupByImplementation == GroupBy::Sort) {
        GroupBySort<Aggregator,T1,T2> groupByOperator(inputGroupBy, inputAggregate);
        groupByOperator.processMicroBatch(n);
        return groupByOperator.getOutput();
    }
    if (groupByImplementation == GroupBy::Adaptive) {
        GroupByAdaptive<Aggregator,T1,T2> groupByOperator(n, inputGroupBy, inputAggregate, cardinality);
        return groupByOperator.processInput();
    }
    if (groupByImplementation == GroupBy::AdaptiveParallel) {
        GroupByAdaptiveParallel<Aggregator,T1,T2> groupByOperator(n, inputGroupBy, inputAggregate, cardinality, dop);
        return groupByOperator.processInput();
    }
    std::cout << "Invalid selection of 'GroupBy' implementation!" << std::endl;
    exit(1);
}

}

#endif //HAQP_GROUPBYIMPLEMENTATION_H

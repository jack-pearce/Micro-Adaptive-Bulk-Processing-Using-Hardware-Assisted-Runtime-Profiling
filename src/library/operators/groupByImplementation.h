#ifndef MABPL_GROUPBYIMPLEMENTATION_H
#define MABPL_GROUPBYIMPLEMENTATION_H


#include <iostream>
#include <limits>
#include <atomic>
#include <condition_variable>
#include "tsl/robin_map.h"

#include "../utilities/systemInformation.h"
#include "../utilities/papi.h"
#include "../machine_constants/machineConstants.h"


namespace MABPL {

#ifdef __AVX2__
constexpr int BITS_PER_GROUPBY_RADIX_PASS = 10;
#else
constexpr int BITS_PER_GROUPBY_RADIX_PASS = 8;
#endif

template<typename T>
inline T MinAggregation<T>::operator()(T currentAggregate, T numberToInclude, bool firstAggregation) const {
    if (firstAggregation) {
        return numberToInclude;
    }
    return std::min(currentAggregate, numberToInclude);
}

template<typename T>
inline T MaxAggregation<T>::operator()(T currentAggregate, T numberToInclude, bool firstAggregation) const {
    if (firstAggregation) {
        return numberToInclude;
    }
    return std::max(currentAggregate, numberToInclude);
}

template<typename T>
inline T SumAggregation<T>::operator()(T currentAggregate, T numberToInclude, bool firstAggregation) const {
    if (firstAggregation) {
        return numberToInclude;
    }
    return currentAggregate + numberToInclude;
}

template<typename T>
inline T CountAggregation<T>::operator()(T currentAggregate, T _, bool firstAggregation) const {
    if (firstAggregation) {
        return 1;
    }
    return ++currentAggregate;
}

template<template<typename> class Aggregator, typename T1, typename T2>
inline void groupByHashAux(int n, T1 *inputGroupBy, T2 *inputAggregate, tsl::robin_map<T1, T2> &map, int &index) {
    typename tsl::robin_map<T1, T2>::iterator it;
    int startingIndex = index;
    for (; index < startingIndex + n; ++index) {
        it = map.find(inputGroupBy[index]);
        if (it != map.end()) {
            it.value() += 1;
//            it.value() = Aggregator<T2>()(it->second, inputAggregate[index], false);
        } else {
            map.insert({inputGroupBy[index], inputAggregate[index]});
//            map.insert({inputGroupBy[index], Aggregator<T2>()(0, inputAggregate[index], true)});
        }
    }
}

template<template<typename> class Aggregator, typename T1, typename T2>
vectorOfPairs<T1, T2> groupByHash(int n, T1 *inputGroupBy, T2 *inputAggregate, int cardinality) {
    static_assert(std::is_integral<T1>::value, "GroupBy column must be an integer type");
    static_assert(std::is_arithmetic<T2>::value, "Payload column must be an numeric type");

    tsl::robin_map<T1, T2> map(std::max(static_cast<int>(2.5 * cardinality), 400000));
    int index = 0;

    int tuplesToProcess;
    while (index < n) {
        tuplesToProcess = std::min(75000, n - index);
        groupByHashAux<Aggregator>(tuplesToProcess, inputGroupBy, inputAggregate, map, index);
    }

//    groupByHashAux<Aggregator>(n, inputGroupBy, inputAggregate, map, index);

    return {map.begin(), map.end()};
}

template<template<typename> class Aggregator, typename T1, typename T2>
void groupBySortAuxAgg(int start, int end, const T1 *inputGroupBy, T2 *inputAggregate, int mask, int numBuckets,
                       std::vector<int> &buckets, vectorOfPairs <T1, T2> &result) {
    int i;
    bool bucketEntryPresent[1 << BITS_PER_GROUPBY_RADIX_PASS] = {false};

    for (i = start; i < end; i++) {
        buckets[inputGroupBy[i] & mask] = Aggregator<T2>()(buckets[inputGroupBy[i] & mask], inputAggregate[i],
                                                           !bucketEntryPresent[inputGroupBy[i] & mask]);
        bucketEntryPresent[inputGroupBy[i] & mask] = true;
    }

    int valuePrefix = inputGroupBy[start] & ~mask;

    for (i = 0; i < numBuckets; i++) {
        if (bucketEntryPresent[i]) {
            result.emplace_back(valuePrefix | i, buckets[i]);
        }
    }

    std::fill(buckets.begin(), buckets.end(), 0);
}

template<template<typename> class Aggregator, typename T1, typename T2>
void groupBySortAux(int start, int end, T1 *inputGroupBy, T2 *inputAggregate, T1 *bufferGroupBy, T2 *bufferAggregate,
                    int mask, int numBuckets, std::vector<int> &buckets, int pass, vectorOfPairs <T1, T2> &result) {
    int i;

    for (i = start; i < end; i++) {
        buckets[(inputGroupBy[i] >> (pass * BITS_PER_GROUPBY_RADIX_PASS)) & mask]++;
    }

    for (i = 1; i < numBuckets; i++) {
        buckets[i] += buckets[i - 1];
    }

    std::vector<int> partitions(buckets.data(), buckets.data() + numBuckets);
    for (i = 0; i < numBuckets; i++) {
        partitions[i] += start;
    }

    for (i = end - 1; i >= start; i--) {
        bufferGroupBy[start + --buckets[(inputGroupBy[i] >> (pass * BITS_PER_GROUPBY_RADIX_PASS)) & mask]] = inputGroupBy[i];
        bufferAggregate[start + buckets[(inputGroupBy[i] >> (pass * BITS_PER_GROUPBY_RADIX_PASS)) & mask]] = inputAggregate[i];
    }

    std::fill(buckets.begin(), buckets.end(), 0);
    std::swap(inputGroupBy, bufferGroupBy);
    std::swap(inputAggregate, bufferAggregate);
    --pass;

    if (pass > 0) {
        if (partitions[0] > start) {
            groupBySortAux<Aggregator>(start, partitions[0], inputGroupBy, inputAggregate,
                                       bufferGroupBy,
                                       bufferAggregate, mask, numBuckets, buckets, pass, result);
        }
        for (i = 1; i < numBuckets; i++) {
            if (partitions[i] > partitions[i - 1]) {
                groupBySortAux<Aggregator>(partitions[i - 1], partitions[i], inputGroupBy,
                                           inputAggregate,
                                           bufferGroupBy, bufferAggregate, mask, numBuckets, buckets,
                                           pass, result);
            }
        }
    } else {
        if (partitions[0] > start) {
            groupBySortAuxAgg<Aggregator>(start, partitions[0], inputGroupBy, inputAggregate,
                                          mask, numBuckets, buckets, result);
        }
        for (i = 1; i < numBuckets; i++) {
            if (partitions[i] > partitions[i - 1]) {
                groupBySortAuxAgg<Aggregator>(partitions[i - 1], partitions[i], inputGroupBy,
                                              inputAggregate,
                                              mask, numBuckets, buckets, result);
            }
        }
    }
}

template<template<typename> class Aggregator, typename T1, typename T2>
vectorOfPairs<T1, T2> groupBySort(int n, T1 *inputGroupBy, T2 *inputAggregate) {
    static_assert(std::is_integral<T1>::value, "GroupBy column must be an integer type");
    static_assert(std::is_arithmetic<T2>::value, "Payload column must be an numeric type");

    int i;
    int numBuckets = 1 << BITS_PER_GROUPBY_RADIX_PASS;
    int mask = numBuckets - 1;
    T1 largest = 0;

    for (i = 0; i < n; i++) {
        if (inputGroupBy[i] > largest) {
            largest = inputGroupBy[i];
        }
    }
    int msbPosition = 0;
    while (largest != 0) {
        largest >>= 1;
        msbPosition++;
    }

    int pass = static_cast<int>(std::ceil(static_cast<double>(msbPosition) / BITS_PER_GROUPBY_RADIX_PASS)) - 1;
    vectorOfPairs<T1, T2> result;

    std::vector<int> buckets(1 << BITS_PER_GROUPBY_RADIX_PASS, 0);
    T1 *bufferGroupBy = new T1[n];
    T2 *bufferAggregate = new T2[n];

    groupBySortAux<Aggregator>(0, n, inputGroupBy, inputAggregate, bufferGroupBy,
                               bufferAggregate, mask, numBuckets, buckets, pass, result);

    delete[]bufferGroupBy;
    delete[]bufferAggregate;

    return result;
}

template<template<typename> class Aggregator, typename T1, typename T2>
inline void groupByAdaptiveAuxHash(int n, T1 *inputGroupBy, T2 *inputAggregate, tsl::robin_map<T1, T2> &map,
                                   int &index, T1 &largest) {
    typename tsl::robin_map<T1, T2>::iterator it;
    int startingIndex = index;
    for (; index < startingIndex + n; ++index) {
        it = map.find(inputGroupBy[index]);
        if (it != map.end()) {
            it.value() += 1;
//            it.value() = Aggregator<T2>()(it->second, inputAggregate[index], false);
        } else {
            map.insert({inputGroupBy[index], inputAggregate[index]});
//            map.insert({inputGroupBy[index], Aggregator<T2>()(0, inputAggregate[index], true)});
            largest = std::max(largest, inputGroupBy[index]);
        }
    }
}

template<template<typename> class Aggregator, typename T1, typename T2>
vectorOfPairs<T1, T2> groupByAdaptiveAuxSort(int n, T1 *inputGroupBy, T2 *inputAggregate,
                                             vectorOfPairs<int, int> &sectionsToBeSorted,
                                             tsl::robin_map<T1, T2> &map, T1 largest,
                                             vectorOfPairs<T1, T2> &result) {
    int i;
    for (const auto& section : sectionsToBeSorted) {
        for (i = section.first; i < section.second; i++) {
            largest = std::max(largest, inputGroupBy[i]);
        }
    }

    int msbPosition = 0;
    while (largest != 0) {
        largest >>= 1;
        msbPosition++;
    }

    int pass = static_cast<int>(std::ceil(static_cast<double>(msbPosition) / BITS_PER_GROUPBY_RADIX_PASS)) - 1;

    int numBuckets = 1 << BITS_PER_GROUPBY_RADIX_PASS;
    std::vector<int> buckets(1 << BITS_PER_GROUPBY_RADIX_PASS, 0);

    int mask = numBuckets - 1;

    T1 *bufferGroupBy = new T1[n];
    T2 *bufferAggregate = new T2[n];


    for (const auto& section : sectionsToBeSorted) {
        for (i = section.first; i < section.second; i++) {
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
    for (const auto& section : vectorOfPairs<int, int>(sectionsToBeSorted.rbegin(), sectionsToBeSorted.rend())) {
        for (i = section.first; i < section.second; i++) {
            bufferGroupBy[--buckets[(inputGroupBy[i] >> (pass * BITS_PER_GROUPBY_RADIX_PASS)) & mask]] = inputGroupBy[i];
            bufferAggregate[buckets[(inputGroupBy[i] >> (pass * BITS_PER_GROUPBY_RADIX_PASS)) & mask]] = inputAggregate[i];
        }
    }

    std::fill(buckets.begin(), buckets.end(), 0);
    std::swap(inputGroupBy, bufferGroupBy);
    std::swap(inputAggregate, bufferAggregate);
    --pass;

    if (pass > 0) {
        if (partitions[0] > 0) {
            groupBySortAux<Aggregator>(0, partitions[0], inputGroupBy, inputAggregate,
                                       bufferGroupBy, bufferAggregate, mask, numBuckets, buckets,
                                       pass, result);
        }
        for (i = 1; i < numBuckets; i++) {
            if (partitions[i] > partitions[i - 1]) {
                groupBySortAux<Aggregator>(partitions[i - 1], partitions[i], inputGroupBy,
                                           inputAggregate, bufferGroupBy, bufferAggregate, mask,
                                           numBuckets, buckets, pass, result);
            }
        }
    } else {
        if (partitions[0] > 0) {
            groupBySortAuxAgg<Aggregator>(0, partitions[0], inputGroupBy, inputAggregate,
                                          mask, numBuckets, buckets, result);
        }
        for (i = 1; i < numBuckets; i++) {
            if (partitions[i] > partitions[i - 1]) {
                groupBySortAuxAgg<Aggregator>(partitions[i - 1], partitions[i], inputGroupBy,
                                              inputAggregate, mask, numBuckets, buckets, result);
            }
        }
    }

    std::swap(inputGroupBy, bufferGroupBy);
    std::swap(inputAggregate, bufferAggregate);

    delete[]bufferGroupBy;
    delete[]bufferAggregate;

    return result;
}

template<template<typename> class Aggregator, typename T1, typename T2>
vectorOfPairs<T1, T2> groupByAdaptive(int n, T1 *inputGroupBy, T2 *inputAggregate, int cardinality) {
    static_assert(std::is_integral<T1>::value, "GroupBy column must be an integer type");
    static_assert(std::is_arithmetic<T2>::value, "Payload column must be an numeric type");

    constexpr int tuplesPerChunk = 75 * 1000;
    constexpr int tuplesBetweenHashing = 2*1000*1000;
    int initialSize = std::max(static_cast<int>(2.5 * cardinality), 400000);

    tsl::robin_map<T1, T2> map(initialSize);
    typename tsl::robin_map<T1, T2>::iterator it;

    std::vector<std::string> counters = {"PERF_COUNT_HW_CACHE_MISSES"};
    long_long *counterValues = Counters::getInstance().getSharedEventSetEvents(counters);

    std::string machineConstantName = "GroupBy_" + std::to_string(sizeof(T1)) + "B_inputFilter_" +
            std::to_string(sizeof(T2)) + "B_inputAggregate_1_dop";
    float tuplesPerLastLevelCacheMissThreshold = MachineConstants::getInstance().getMachineConstant(machineConstantName);

    int index = 0;
    int tuplesToProcess;

    vectorOfPairs<int, int> sectionsToBeSorted;
    int elements = 0;

    vectorOfPairs<T1, T2> result;
    T1 mapLargest = std::numeric_limits<T1>::lowest();

    while (index < n) {

        tuplesToProcess = std::min(tuplesPerChunk, n - index);

        Counters::getInstance().readSharedEventSet();

        groupByAdaptiveAuxHash<Aggregator>(tuplesToProcess, inputGroupBy, inputAggregate, map, index, mapLargest);

        Counters::getInstance().readSharedEventSet();

        if ((static_cast<float>(tuplesToProcess) / counterValues[0]) < tuplesPerLastLevelCacheMissThreshold) {
//            std::cout << "Switched to sort at index " << index << std::endl;
            tuplesToProcess = std::min(tuplesBetweenHashing, n - index);

            sectionsToBeSorted.emplace_back(index, index + tuplesToProcess);
            index += tuplesToProcess;
            elements += tuplesToProcess;
        }
    }

    if (sectionsToBeSorted.empty()) {
        return {map.begin(), map.end()};
    }
    elements += map.size();
    return groupByAdaptiveAuxSort<Aggregator>(elements, inputGroupBy, inputAggregate, sectionsToBeSorted,
                                              map, mapLargest, result);
}

template<typename T1, typename T2>
struct GroupByThreadArgs {
    int n;
    T1* inputGroupBy;
    T2* inputAggregate;
    int cardinality;
    vectorOfPairs<T1, T2>* result;
    int threadNumber;
    std::atomic<int> *numFinishedThreads;
    std::vector<bool> *threadFinishedAndWaitingFlags;
    std::condition_variable *cv;
    GroupByThreadArgs *mergeThread1;
    GroupByThreadArgs *mergeThread2;
    int dop;

    ~GroupByThreadArgs() {
        delete result;
    }

    vectorOfPairs<T1, T2> extractResult() {
        vectorOfPairs<T1, T2>* tmp = result;
        result = nullptr;
        return vectorOfPairs<T1, T2>(std::move(*tmp));
    }
};

template<template<typename> class Aggregator, typename T1, typename T2>
void *groupByAdaptiveParallelPerformMerge(void *arg) {
    auto* args = static_cast<GroupByThreadArgs<T1, T2>*>(arg);
    std::vector<bool> *threadFinishedAndWaitingFlags = args->threadFinishedAndWaitingFlags;
    std::atomic<int> *numFinishedThreads = args->numFinishedThreads;
    std::condition_variable *cv = args->cv;
    GroupByThreadArgs<T1, T2> *mergeThread1 = args->mergeThread1;
    GroupByThreadArgs<T1, T2> *mergeThread2 = args->mergeThread2;

    vectorOfPairs<T1,T2> *inputResult1 = mergeThread1->result;
    vectorOfPairs<T1,T2> *inputResult2 = mergeThread2->result;
    vectorOfPairs<T1,T2> *result = args->result;

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
vectorOfPairs<T1, T2> groupByAdaptiveParallelMerge(std::condition_variable &cv, std::mutex &cvMutex,
                                                   std::atomic<int> &numFinishedThreads,
                                                   std::vector<bool> &threadFinishedAndWaitingFlags, int dop,
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
        threadArgs[threadNumber]->result = new vectorOfPairs<T1, T2>;
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

template<template<typename> class Aggregator, typename T1, typename T2>
void *groupByAdaptiveParallelAux(void *arg) {
    auto* args = static_cast<GroupByThreadArgs<T1, T2>*>(arg);
    int n = args->n;
    T1 *inputGroupBy = args->inputGroupBy;
    T2 *inputAggregate = args->inputAggregate;
    int cardinality = args->cardinality;
    std::vector<bool> *threadFinishedAndWaitingFlags = args->threadFinishedAndWaitingFlags;
    std::atomic<int> *numFinishedThreads = args->numFinishedThreads;
    std::condition_variable *cv = args->cv;
    int dop = args->dop;

    constexpr int tuplesPerChunk = 75 * 1000;
    constexpr int tuplesBetweenHashing = 2*1000*1000;
    int maxCardinality = std::min(cardinality, n);
    int initialSize = std::max(static_cast<int>(2.5 * maxCardinality), 400000);

    tsl::robin_map<T1, T2> map(initialSize);
    typename tsl::robin_map<T1, T2>::iterator it;

    int eventSet = PAPI_NULL;
    std::vector<std::string> counters = {"PERF_COUNT_HW_CACHE_MISSES"};
    long_long counterValues[1] = {0};
    createThreadEventSet(&eventSet, counters);

    std::string machineConstantName = "GroupBy_" + std::to_string(sizeof(T1)) + "B_inputFilter_" +
                              std::to_string(sizeof(T2)) + "B_inputAggregate_" + std::to_string(dop) + "_dop";
    float tuplesPerLastLevelCacheMissThreshold = MachineConstants::getInstance().getMachineConstant(machineConstantName);

    int index = 0;
    int tuplesToProcess;

    vectorOfPairs<int, int> sectionsToBeSorted;
    int elements = 0;

    T1 mapLargest = std::numeric_limits<T1>::lowest();

    while (index < n) {

        tuplesToProcess = std::min(tuplesPerChunk, n - index);

        readThreadEventSet(eventSet, 1, counterValues);

        groupByAdaptiveAuxHash<Aggregator>(tuplesToProcess, inputGroupBy, inputAggregate, map, index, mapLargest);

        readThreadEventSet(eventSet, 1, counterValues);

        if ((static_cast<float>(tuplesToProcess) / counterValues[0]) < tuplesPerLastLevelCacheMissThreshold) {
            tuplesToProcess = std::min(tuplesBetweenHashing, n - index);

            sectionsToBeSorted.emplace_back(index, index + tuplesToProcess);
            index += tuplesToProcess;
            elements += tuplesToProcess;
        }
    }

    destroyThreadEventSet(eventSet, counterValues);

    if (sectionsToBeSorted.empty()) {
        args->result->reserve(map.size());
        args->result->insert(args->result->end(), map.begin(), map.end());
        sortVectorOfPairs(args->result);
    } else {
        elements += map.size();
        groupByAdaptiveAuxSort<Aggregator>(elements, inputGroupBy, inputAggregate,
                                           sectionsToBeSorted,map, mapLargest, *(args->result));
    }
    (*threadFinishedAndWaitingFlags)[args->threadNumber] = true;
    (*numFinishedThreads)++;
    cv->notify_one();
    return nullptr;
}

template<template<typename> class Aggregator, typename T1, typename T2>
vectorOfPairs<T1, T2> groupByAdaptiveParallel(int n, T1 *inputGroupBy, T2 *inputAggregate, int cardinality, int dop) {
    static_assert(std::is_integral<T1>::value, "GroupBy column must be an integer type");
    static_assert(std::is_arithmetic<T2>::value, "Payload column must be an numeric type");
    assert(1 < dop && dop <= logicalCoresCount());
    int totalThreads = (dop * 2) - 1;

    Counters::getInstance();
    MachineConstants::getInstance();
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
        threadArgs[i]->result = new vectorOfPairs<T1, T2>;
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
    return groupByAdaptiveParallelMerge<Aggregator, T1, T2>(cv, cvMutex, numFinishedThreads,
                                                            threadFinishedAndWaitingFlags, dop, threadArgs, threads);
}

template<template<typename> class Aggregator, typename T1, typename T2>
vectorOfPairs<T1, T2> runGroupByFunction(GroupBy groupByImplementation, int n, T1 *inputGroupBy, T2 *inputAggregate,
                                         int cardinality, int dop) {
    switch (groupByImplementation) {
        case GroupBy::Hash:
            return groupByHash<Aggregator>(n, inputGroupBy, inputAggregate, cardinality);
        case GroupBy::Sort:
            return groupBySort<Aggregator>(n, inputGroupBy, inputAggregate);
        case GroupBy::Adaptive:
            return groupByAdaptive<Aggregator>(n, inputGroupBy, inputAggregate, cardinality);
        case GroupBy::AdaptiveParallel:
            return groupByAdaptiveParallel<Aggregator>(n, inputGroupBy, inputAggregate, cardinality, dop);
        default:
            std::cout << "Invalid selection of 'GroupBy' implementation!" << std::endl;
            exit(1);
    }
}

}

#endif //MABPL_GROUPBYIMPLEMENTATION_H

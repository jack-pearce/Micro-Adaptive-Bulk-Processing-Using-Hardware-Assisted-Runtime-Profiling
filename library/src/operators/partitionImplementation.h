#ifndef HAQP_PARTITIONIMPLEMENTATION_H
#define HAQP_PARTITIONIMPLEMENTATION_H


#include <vector>
#include <cmath>
#include <iostream>
#include <limits>

#include "utilities/systemInformation.h"
#include "utilities/papiWrapper.h"
#include "machine_constants/machineConstants.h"


namespace HAQP {

/****************************** FORWARD DECLARATIONS ******************************/

template<typename T>
class MonitorPartition {
public:
    MonitorPartition();
    bool robustnessIncreaseRequired(int tuplesProcessed);

private:
    const long_long *sTlbStoreMisses;
    float tuplesPerTlbStoreMiss;
};

/****************************** FOUNDATIONAL ALGORITHMS ******************************/

template <typename T>
class Partition {
public:
    Partition(int n, T *keys, int radixBits = 0) :
            nInput(n),
            keysInput(keys),
            radixBitsOperator(radixBits) {

        static_assert(std::is_integral<T>::value, "PartitionOperators column must be an integer type");

        if (radixBitsOperator == 0) {
            std::string startName = "Partition_startRadixBits";
            radixBitsOperator = static_cast<int>(MachineConstants::getInstance().getMachineConstant(startName));
        }
        assert(radixBitsOperator > 0);

        std::string minName = "Partition_minRadixBits";
        minimumRadixBits = static_cast<int>(MachineConstants::getInstance().getMachineConstant(minName));

        T largest = std::numeric_limits<T>::min();;
        for (int i = 0; i < n; i++) {
            largest = std::max(largest, keys[i]);
        }

        msbToPartitionInput = 0;
        while (largest != 0) {
            largest >>= 1;
            msbToPartitionInput++;
        }

        maxElementsPerPartition = static_cast<double>(l3cacheSize()) / (sizeof(T) * 2 * 2.5);

        buckets = std::vector<int>(1 + (1 << radixBitsOperator), 0);
        bufferOperator = new T[n];
    }

    void processInput() {
        performPartition(nInput, keysInput, bufferOperator, msbToPartitionInput, radixBitsOperator, 0, true);
    }

    inline void performPartition(int n, T *keys, T *buffer, int msbToPartition, int radixBits, int offset,
                                 bool copyRequired) {
        radixBits = std::min(msbToPartition, radixBits);
        int shifts = msbToPartition - radixBits;
        int numBuckets = 1 << radixBits;
        unsigned int mask = numBuckets - 1;

        int i;
        for (i = 0; i < n; i++) {
            buckets[1 + ((keys[i] >> shifts) & mask)]++;
        }

        for (i = 2; i <= numBuckets; i++) {
            buckets[i] += buckets[i - 1];
        }

        std::vector<int> partitions(buckets.data() + 1, buckets.data() + numBuckets + 1);

        for (i = 0; i < n; i++) {
            buffer[buckets[(keys[i] >> shifts) & mask]++] = keys[i];
        }

        std::fill(buckets.begin(), buckets.end(), 0);
        msbToPartition -= radixBits;

        if (msbToPartition == 0) {   // No ability to partition further, so return early
            if (copyRequired) {
                memcpy(keys, buffer, n * sizeof(T));
            }
            for (int partitionEnd : partitions) {
                outputPartitions.push_back(offset + partitionEnd);
            }
            return;
        }

        int prevPartitionEnd = 0;
        for (int partitionEnd : partitions) {
            if ((partitionEnd - prevPartitionEnd) > maxElementsPerPartition) {
                performPartition(partitionEnd - prevPartitionEnd, buffer + prevPartitionEnd, keys + prevPartitionEnd,
                                 msbToPartition, radixBits, offset + prevPartitionEnd, !copyRequired);
            } else {
                if (copyRequired) {
                    memcpy(keys + prevPartitionEnd, buffer + prevPartitionEnd,
                           (partitionEnd- prevPartitionEnd) * sizeof(T));
                }
                outputPartitions.push_back(offset + partitionEnd);
            }
            prevPartitionEnd = partitionEnd;
        }
    }

    void processInputAdaptively(MonitorPartition<T> *monitor_) {
        monitor = monitor_;
        tuplesPerHazardCheck = 10 * 1000;
        performPartitionAdaptively(nInput, keysInput, bufferOperator, msbToPartitionInput, radixBitsOperator, 0, true);
    }

    inline void performPartitionAdaptively(int n, T *keys, T *buffer, int msbToPartition, int radixBits, int offset,
                                           bool copyRequired) {
        radixBits = std::min(msbToPartition, radixBits);
        int shifts = msbToPartition - radixBits;
        int numBuckets = 1 << radixBits;
        unsigned int mask = numBuckets - 1;

        int i, microBatchStart, microBatchSize;
        for (i = 0; i < n; i++) {
            buckets[1 + ((keys[i] >> shifts) & mask)]++;
        }

        for (i = 2; i <= numBuckets; i++) {
            buckets[i] += buckets[i - 1];
        }

        std::vector<int> partitions(buckets.data() + 1, buckets.data() + numBuckets + 1);

        i = 0;
        if (radixBits > minimumRadixBits) {
            while (i < n) {
                microBatchSize = std::min(tuplesPerHazardCheck, n - i);
                microBatchStart = i;

                Counters::getInstance().readSharedEventSet();

                for (; i < microBatchStart + microBatchSize; i++) {     // Run chunk
                    buffer[buckets[(keys[i] >> shifts) & mask]++] = keys[i];
                }

                Counters::getInstance().readSharedEventSet();

                if (monitor->robustnessIncreaseRequired(microBatchSize)) {
                    --radixBits;
                    ++shifts;
                    numBuckets >>= 1;
                    mask = numBuckets - 1;

                    /////////////////////////////////////// ADAPTIVITY OUTPUT ///////////////////////////////////////////
//                    std::cout << "RadixBits reduced to " << radixBitsOperator << " after tuple " << i << " due to reading of ";
//                    std::cout << (static_cast<float>(microBatchSize) / static_cast<float>(counterValues[0]));
//                    std::cout << " tuples per TLB store miss" << std::endl;
                    /////////////////////////////////////////////////////////////////////////////////////////////////////

                    mergePartitions(buffer, partitions, numBuckets);

                    if (radixBits == minimumRadixBits) {        // Complete partitioning to avoid unnecessary checks
                        for (; i < n; i++) {
                            buffer[buckets[(keys[i] >> shifts) & mask]++] = keys[i];
                        }
                        break;
                    }
                }
            }
        } else {
            for (; i < n; i++) {
                buffer[buckets[(keys[i] >> shifts) & mask]++] = keys[i];
            }
        }

        std::fill(buckets.begin(), buckets.end(), 0);
        msbToPartition -= radixBits;

        if (msbToPartition == 0) {                                 // No ability to partition further, so return early
            if (copyRequired) {
                memcpy(keys, buffer, n * sizeof(T));
            }
            for (int partitionEnd : partitions) {
                outputPartitions.push_back(offset + partitionEnd);
            }
            return;
        }

        int prevPartitionEnd = 0;
        for (int partitionEnd : partitions) {
            if ((partitionEnd - prevPartitionEnd) > maxElementsPerPartition) {
                performPartitionAdaptively(partitionEnd - prevPartitionEnd, buffer + prevPartitionEnd,
                                           keys + prevPartitionEnd, msbToPartition, radixBits,
                                           offset + prevPartitionEnd, !copyRequired);
            } else {
                if (copyRequired) {
                    memcpy(keys + prevPartitionEnd, buffer + prevPartitionEnd,
                           (partitionEnd - prevPartitionEnd) * sizeof(T));
                }
                outputPartitions.push_back(offset + partitionEnd);
            }
            prevPartitionEnd = partitionEnd;
        }
    }

    inline void mergePartitions(T *buffer, std::vector<int> &partitions, int numBuckets) {
        for (int j = 0; j < numBuckets; ++j) {      // Move values in buffer
            memcpy(&buffer[buckets[j << 1]],
                   &buffer[partitions[j << 1]],
                   (buckets[(j << 1) + 1] - partitions[j << 1]) * sizeof(T));
        }

        for (int j = 0; j < numBuckets; ++j) {      // Merge histogram values and reduce size
            buckets[j] = buckets[j << 1] + (buckets[(j << 1) + 1] - partitions[j << 1]);
        }
        buckets.resize(1 + numBuckets);

        for (int j = 1; j <= numBuckets; ++j) {     // Merge partitions and reduce size
            partitions[j - 1] = partitions[(j << 1) - 1];
        }
        partitions.resize(numBuckets);
    }

    std::vector<int> getOutput() {
        delete []bufferOperator;
        return outputPartitions;
    }

private:
    int nInput;
    T *keysInput;
    T* bufferOperator;
    int minimumRadixBits;
    int radixBitsOperator;
    int msbToPartitionInput;

    int maxElementsPerPartition;
    std::vector<int> buckets;
    std::vector<int> outputPartitions;

    MonitorPartition<T> *monitor;
    int tuplesPerHazardCheck {};
};

/****************************** SINGLE-THREADED ******************************/

template<typename T>
MonitorPartition<T>::MonitorPartition() : tuplesPerTlbStoreMiss(50.0) {
    std::vector<std::string> counters = {"DTLB-STORE-MISSES"};
        sTlbStoreMisses = Counters::getInstance().getSharedEventSetEvents(counters);
}

template<typename T>
bool MonitorPartition<T>::robustnessIncreaseRequired(int tuplesProcessed) {
    return (static_cast<float>(tuplesProcessed) / *sTlbStoreMisses) < tuplesPerTlbStoreMiss;
}

template<typename T>
class PartitionAdaptive {
public:
    PartitionAdaptive(int n, T *keys);
    std::vector<int> processInput();

private:
    Partition<T> partitionOperator;
    MonitorPartition<T> monitor;
};

template<typename T>
PartitionAdaptive<T>::PartitionAdaptive(int n, T *keys) :
        partitionOperator(Partition<T>(n, keys)),
        monitor(MonitorPartition<T>()) {}

template<typename T>
std::vector<int> PartitionAdaptive<T>::processInput() {
    partitionOperator.processInputAdaptively(&monitor);
    return partitionOperator.getOutput();
}

template<typename T>
std::vector<int> runPartitionFunction(PartitionOperators partitionImplementation, int n, T *keys, int radixBits) {
    if (partitionImplementation == PartitionOperators::RadixBitsFixed) {
        Partition<T> partitionOperator(n, keys, radixBits);
        partitionOperator.processInput();
        return partitionOperator.getOutput();
    }
    if (partitionImplementation == PartitionOperators::RadixBitsAdaptive) {
        PartitionAdaptive<T> partitionOperator(n, keys);
        return partitionOperator.processInput();
    }
    std::cout << "Invalid selection of 'PartitionOperators' implementation!" << std::endl;
    exit(1);
}


}

#endif //HAQP_PARTITIONIMPLEMENTATION_H

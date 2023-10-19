#ifndef MABPL_MACHINECONSTANTSIMPLEMENTATION_H
#define MABPL_MACHINECONSTANTSIMPLEMENTATION_H

#include <iostream>

#include "utilities/dataGeneration.h"
#include "utilities/papiWrapper.h"
#include "operators/select.h"
#include "operators/groupBy.h"

namespace MABPL {

constexpr int NUMBER_OF_TESTS = 9;
constexpr int SELECT_ITERATIONS = 12;
constexpr int GROUPBY_ITERATIONS = 12;


template <typename T>
T getThreshold(int n, double selectivity) {
    return static_cast<T>(n * selectivity);
}

template <typename T>
double calculateSelectIndexesLowerMachineConstant(int dop) {
    int n = 250*1000*1000;
    auto data = new T[n];
    generateRandomisedUniqueValuesInMemory(data, n);

    long_long branchCycles, predicationCycles;
    double upperSelectivity = 0.5;
    double lowerSelectivity = 0;
    double midSelectivity;

    std::string machineConstantLowerName = "SelectIndexesLower_" + std::to_string(sizeof(T)) + "B_inputFilter_" + std::to_string(dop) + "_dop";
    std::string machineConstantUpperName = "SelectIndexesUpper_" + std::to_string(sizeof(T)) + "B_inputFilter_" + std::to_string(dop) + "_dop";

    for (int i = 0; i < SELECT_ITERATIONS; ++i) {

        midSelectivity = (lowerSelectivity + upperSelectivity) / 2;

        auto inputFilter = new T[n];
        auto selection = new int[n];
        int* inputData;
        copyArray(data, inputFilter, n);

        if (dop == 1) {
            branchCycles = *Counters::getInstance().readSharedEventSet();
            runSelectFunction(Select::ImplementationIndexesBranch, n, inputData, inputFilter,
                              selection, getThreshold<T>(n, midSelectivity));
            branchCycles = *Counters::getInstance().readSharedEventSet() - branchCycles;
        } else {
            MachineConstants::getInstance().updateMachineConstant(machineConstantLowerName, 1);
            MachineConstants::getInstance().updateMachineConstant(machineConstantUpperName, 0);
            branchCycles = PAPI_get_real_usec();
            runSelectFunction(Select::ImplementationIndexesAdaptiveParallel, n, inputData, inputFilter,
                              selection, getThreshold<T>(n, midSelectivity), dop);
            branchCycles = PAPI_get_real_usec() - branchCycles;
        }

        delete[] inputFilter;
        delete[] selection;

        inputFilter = new T[n];
        selection = new int[n];
        copyArray(data, inputFilter, n);

        if (dop == 1) {
            predicationCycles = *Counters::getInstance().readSharedEventSet();
            runSelectFunction(Select::ImplementationIndexesPredication, n, inputData, inputFilter,
                              selection, getThreshold<T>(n, midSelectivity));
            predicationCycles = *Counters::getInstance().readSharedEventSet() - predicationCycles;
        } else {
            MachineConstants::getInstance().updateMachineConstant(machineConstantLowerName, 0);
            MachineConstants::getInstance().updateMachineConstant(machineConstantUpperName, 1);
            predicationCycles = PAPI_get_real_usec();
            runSelectFunction(Select::ImplementationIndexesAdaptiveParallel, n, inputData, inputFilter,
                              selection, getThreshold<T>(n, midSelectivity), dop);
            predicationCycles = PAPI_get_real_usec() - predicationCycles;
        }

        delete[] inputFilter;
        delete[] selection;

        if (branchCycles > predicationCycles) {
            upperSelectivity = midSelectivity;
        } else {
            lowerSelectivity = midSelectivity;
        }

        //        std::cout << "Selectivity: " << (lowerSelectivity + upperSelectivity) / 2 << ", branch cycles: " << branchCycles << ", predication cycles: " << predicationCycles << std::endl;
    }

    std::cout << ".";
    std::cout.flush();

    delete[] data;

    return (upperSelectivity + lowerSelectivity) / 2;
}

template <typename T>
double calculateSelectIndexesUpperMachineConstant(int dop) {
    int n = 250*1000*1000;
    auto data = new T[n];
    generateRandomisedUniqueValuesInMemory(data, n);

    long_long branchCycles, predicationCycles;
    double upperSelectivity = 1.0;
    double lowerSelectivity = 0.5;
    double midSelectivity;

    std::string machineConstantLowerName = "SelectIndexesLower_" + std::to_string(sizeof(T)) + "B_inputFilter_" + std::to_string(dop) + "_dop";
    std::string machineConstantUpperName = "SelectIndexesUpper_" + std::to_string(sizeof(T)) + "B_inputFilter_" + std::to_string(dop) + "_dop";

    for (int i = 0; i < SELECT_ITERATIONS; ++i) {

        midSelectivity = (lowerSelectivity + upperSelectivity) / 2;

        auto inputFilter = new T[n];
        auto selection = new int[n];
        int* inputData;
        copyArray(data, inputFilter, n);

        if (dop == 1) {
            branchCycles = *Counters::getInstance().readSharedEventSet();
            runSelectFunction(Select::ImplementationIndexesBranch, n, inputData, inputFilter,
                              selection, getThreshold<T>(n, midSelectivity));
            branchCycles = *Counters::getInstance().readSharedEventSet() - branchCycles;
        } else {
            MachineConstants::getInstance().updateMachineConstant(machineConstantLowerName, 1);
            MachineConstants::getInstance().updateMachineConstant(machineConstantUpperName, 0);
            branchCycles = PAPI_get_real_usec();
            runSelectFunction(Select::ImplementationIndexesAdaptiveParallel, n, inputData, inputFilter,
                              selection, getThreshold<T>(n, midSelectivity), dop);
            branchCycles = PAPI_get_real_usec() - branchCycles;
        }

        delete[] inputFilter;
        delete[] selection;

        inputFilter = new T[n];
        selection = new int[n];
        copyArray(data, inputFilter, n);

        if (dop == 1) {
            predicationCycles = *Counters::getInstance().readSharedEventSet();
            runSelectFunction(Select::ImplementationIndexesPredication, n, inputData, inputFilter,
                              selection, getThreshold<T>(n, midSelectivity));
            predicationCycles = *Counters::getInstance().readSharedEventSet() - predicationCycles;
        } else {
            MachineConstants::getInstance().updateMachineConstant(machineConstantLowerName, 0);
            MachineConstants::getInstance().updateMachineConstant(machineConstantUpperName, 1);
            predicationCycles = PAPI_get_real_usec();
            runSelectFunction(Select::ImplementationIndexesAdaptiveParallel, n, inputData, inputFilter,
                              selection, getThreshold<T>(n, midSelectivity), dop);
            predicationCycles = PAPI_get_real_usec() - predicationCycles;
        }

        delete[] inputFilter;
        delete[] selection;

        if (branchCycles > predicationCycles) {
            lowerSelectivity = midSelectivity;
        } else {
            upperSelectivity = midSelectivity;
        }

//        std::cout << "Selectivity: " << (lowerSelectivity + upperSelectivity) / 2 << ", branch cycles: " << branchCycles << ", predication cycles: " << predicationCycles << std::endl;
    }

    std::cout << ".";
    std::cout.flush();

    delete[] data;

    return (upperSelectivity + lowerSelectivity) / 2;
}

template <typename T>
void calculateSelectIndexesMachineConstants(int dop) {
    std::cout << "Calculating machine constants for SelectIndexes_" << sizeof(T) << "B_inputFilter_" << std::to_string(dop) << "_dop" << std::endl;
    std::cout << " - Running tests for lower crossover point";

    std::vector<double> lowerCrossoverPoints;
    for (int i = 0; i < NUMBER_OF_TESTS; ++i) {
        lowerCrossoverPoints.push_back(calculateSelectIndexesLowerMachineConstant<T>(dop));
    }
    std::sort(lowerCrossoverPoints.begin(), lowerCrossoverPoints.end());
    std::cout << " Complete" << std::endl;

    std::cout << " - Running tests for upper crossover point";
    std::vector<double> upperCrossoverPoints;
    for (int i = 0; i < NUMBER_OF_TESTS; ++i) {
        upperCrossoverPoints.push_back(calculateSelectIndexesUpperMachineConstant<T>(dop));
    }
    std::sort(upperCrossoverPoints.begin(), upperCrossoverPoints.end());
    std::cout << " Complete" << std::endl;

    std::string machineConstantLowerName = "SelectIndexesLower_" + std::to_string(sizeof(T)) + "B_inputFilter_" + std::to_string(dop) + "_dop";
    std::string machineConstantUpperName = "SelectIndexesUpper_" + std::to_string(sizeof(T)) + "B_inputFilter_" + std::to_string(dop) + "_dop";

    MachineConstants::getInstance().updateMachineConstant(machineConstantLowerName,
                                                          lowerCrossoverPoints[NUMBER_OF_TESTS / 2]);
    MachineConstants::getInstance().updateMachineConstant(machineConstantUpperName,
                                                          upperCrossoverPoints[NUMBER_OF_TESTS / 2]);
}

template <typename T1, typename T2>
double calculateSelectValuesLowerMispredictionsMachineConstant(int dop) {
    int n = 250*1000*1000;
    auto inputFilterData = new T1[n];
    auto inputDataData = new T2[n];
    generateRandomisedUniqueValuesInMemory(inputFilterData, n);
    generateRandomisedUniqueValuesInMemory(inputDataData, n);

    long_long branchCycles, vectorizationCycles;
    double upperSelectivity = 0.5;
    double lowerSelectivity = 0;
    double midSelectivity;

    std::string machineConstantMispredictionsName = "SelectValuesLowerMispredictions_" + std::to_string(sizeof(T1))
                + "B_inputFilter_" + std::to_string(sizeof(T2)) + "B_inputData_" + std::to_string(dop) + "_dop";
    std::string machineConstantSelectivityName = "SelectValuesLowerSelectivity_" + std::to_string(sizeof(T1))
                + "B_inputFilter_" + std::to_string(sizeof(T2)) + "B_inputData_" + std::to_string(dop) + "_dop";

    for (int i = 0; i < SELECT_ITERATIONS; ++i) {

        midSelectivity = (lowerSelectivity + upperSelectivity) / 2;

        auto inputFilter = new T1[n];
        auto inputData = new T2[n];
        auto selection = new T2[n];
        copyArray(inputFilterData, inputFilter, n);
        copyArray(inputDataData, inputData, n);

        if (dop == 1) {
            branchCycles = *Counters::getInstance().readSharedEventSet();
            runSelectFunction(Select::ImplementationValuesBranch, n, inputData, inputFilter,
                              selection, getThreshold<T1>(n, midSelectivity));
            branchCycles = *Counters::getInstance().readSharedEventSet() - branchCycles;
        } else {
            MachineConstants::getInstance().updateMachineConstant(machineConstantMispredictionsName, 1);
            MachineConstants::getInstance().updateMachineConstant(machineConstantSelectivityName, 1);
            branchCycles = PAPI_get_real_usec();
            runSelectFunction(Select::ImplementationValuesAdaptiveParallel, n, inputData, inputFilter,
                              selection, getThreshold<T1>(n, midSelectivity), dop);
            branchCycles = PAPI_get_real_usec() - branchCycles;
        }

        delete[] inputFilter;
        delete[] inputData;
        delete[] selection;

        inputFilter = new T1[n];
        inputData = new T2[n];;
        selection = new T2[n];
        copyArray(inputFilterData, inputFilter, n);
        copyArray(inputDataData, inputData, n);

        if (dop == 1) {
            vectorizationCycles = *Counters::getInstance().readSharedEventSet();
            runSelectFunction(Select::ImplementationValuesVectorized, n, inputData, inputFilter,
                              selection, getThreshold<T1>(n, midSelectivity));
            vectorizationCycles = *Counters::getInstance().readSharedEventSet() - vectorizationCycles;
        } else {
            MachineConstants::getInstance().updateMachineConstant(machineConstantMispredictionsName, 0);
            MachineConstants::getInstance().updateMachineConstant(machineConstantSelectivityName, 0);
            vectorizationCycles = PAPI_get_real_usec();
            runSelectFunction(Select::ImplementationValuesAdaptiveParallel, n, inputData, inputFilter,
                              selection, getThreshold<T1>(n, midSelectivity), dop);
            vectorizationCycles = PAPI_get_real_usec() - vectorizationCycles;
        }

        delete[] inputFilter;
        delete[] inputData;
        delete[] selection;

        if (branchCycles > vectorizationCycles) {
            upperSelectivity = midSelectivity;
        } else {
            lowerSelectivity = midSelectivity;
        }

//        std::cout << "Selectivity: " << (lowerSelectivity + upperSelectivity) / 2 << ", branch cycles: " << branchCycles << ", vectorization cycles: " << vectorizationCycles << std::endl;
    }

    std::cout << ".";
    std::cout.flush();

    delete[] inputFilterData;
    delete[] inputDataData;

    return (upperSelectivity + lowerSelectivity) / 2;
}

template <typename T1, typename T2>
double calculateSelectValuesLowerSelectivityMachineConstant(int dop) {
    int n = 250*1000*1000;
    int upperBound = 100;
    auto inputFilterData = new T1[n];
    auto inputDataData = new T2[n];
    generatePartiallySortedInMemory(inputFilterData, n, 100, 0);
    generateRandomisedUniqueValuesInMemory(inputDataData, n);

    long_long branchCycles, vectorizationCycles;
    double upperSelectivity = 1;
    double lowerSelectivity = 0;
    double midSelectivity;

    std::string machineConstantMispredictionsName = "SelectValuesLowerMispredictions_" + std::to_string(sizeof(T1))
                + "B_inputFilter_" + std::to_string(sizeof(T2)) + "B_inputData_" + std::to_string(dop) + "_dop";
    std::string machineConstantSelectivityName = "SelectValuesLowerSelectivity_" + std::to_string(sizeof(T1))
                + "B_inputFilter_" + std::to_string(sizeof(T2)) + "B_inputData_" + std::to_string(dop) + "_dop";

    for (int i = 0; i < SELECT_ITERATIONS; ++i) {

        midSelectivity = (lowerSelectivity + upperSelectivity) / 2;

        auto inputFilter = new T1[n];
        auto inputData = new T2[n];
        auto selection = new T2[n];
        copyArray(inputFilterData, inputFilter, n);
        copyArray(inputDataData, inputData, n);

        if (dop == 1) {
            branchCycles = *Counters::getInstance().readSharedEventSet();
            runSelectFunction(Select::ImplementationValuesBranch, n, inputData, inputFilter,
                              selection, getThreshold<T1>(upperBound, midSelectivity));
            branchCycles = *Counters::getInstance().readSharedEventSet() - branchCycles;
        } else {
            MachineConstants::getInstance().updateMachineConstant(machineConstantMispredictionsName, 1);
            MachineConstants::getInstance().updateMachineConstant(machineConstantSelectivityName, 1);
            branchCycles = PAPI_get_real_usec();
            runSelectFunction(Select::ImplementationValuesAdaptiveParallel, n, inputData, inputFilter,
                              selection, getThreshold<T1>(upperBound, midSelectivity), dop);
            branchCycles = PAPI_get_real_usec() - branchCycles;
        }

        delete[] inputFilter;
        delete[] inputData;
        delete[] selection;

        inputFilter = new T1[n];
        inputData = new T2[n];;
        selection = new T2[n];
        copyArray(inputFilterData, inputFilter, n);
        copyArray(inputDataData, inputData, n);

        if (dop == 1) {
            vectorizationCycles = *Counters::getInstance().readSharedEventSet();
            runSelectFunction(Select::ImplementationValuesVectorized, n, inputData, inputFilter,
                              selection, getThreshold<T1>(upperBound, midSelectivity));
            vectorizationCycles = *Counters::getInstance().readSharedEventSet() - vectorizationCycles;
        } else {
            MachineConstants::getInstance().updateMachineConstant(machineConstantMispredictionsName, 0);
            MachineConstants::getInstance().updateMachineConstant(machineConstantSelectivityName, 0);
            vectorizationCycles = PAPI_get_real_usec();
            runSelectFunction(Select::ImplementationValuesAdaptiveParallel, n, inputData, inputFilter,
                              selection, getThreshold<T1>(upperBound, midSelectivity), dop);
            vectorizationCycles = PAPI_get_real_usec() - vectorizationCycles;
        }

        delete[] inputFilter;
        delete[] inputData;
        delete[] selection;

        if (branchCycles > vectorizationCycles) {
            upperSelectivity = midSelectivity;
        } else {
            lowerSelectivity = midSelectivity;
        }

//        std::cout << "Selectivity: " << (lowerSelectivity + upperSelectivity) / 2 << ", branch cycles: " << branchCycles << ", vectorization cycles: " << vectorizationCycles << std::endl;
    }

    std::cout << ".";
    std::cout.flush();

    delete[] inputFilterData;
    delete[] inputDataData;

    return (upperSelectivity + lowerSelectivity) / 2;
}

template <typename T1, typename T2>
void calculateSelectValuesMachineConstants(int dop) {
    std::cout << "Calculating machine constants for SelectValues_" << sizeof(T1) << "B_inputFilter_";
    std::cout << sizeof(T2) << "B_inputAggregate_" << std::to_string(dop) << "_dop" << std::endl;
    std::cout << " - Running tests for lower branch mispredictions crossover point";

    std::vector<double> lowerMisPredictionsCrossoverPoints;
    for (int i = 0; i < NUMBER_OF_TESTS; ++i) {
        lowerMisPredictionsCrossoverPoints.push_back(calculateSelectValuesLowerMispredictionsMachineConstant<T1, T2>(dop));
    }
    std::sort(lowerMisPredictionsCrossoverPoints.begin(), lowerMisPredictionsCrossoverPoints.end());
    std::cout << " Complete" << std::endl;

    std::cout << " - Running tests for lower selectivity crossover point";
    std::vector<double> lowerSelectivityCrossoverPoints;
    for (int i = 0; i < NUMBER_OF_TESTS; ++i) {
        lowerSelectivityCrossoverPoints.push_back(calculateSelectValuesLowerSelectivityMachineConstant<T1, T2>(dop));
    }
    std::sort(lowerSelectivityCrossoverPoints.begin(), lowerSelectivityCrossoverPoints.end());
    std::cout << " Complete" << std::endl;

    std::string machineConstantMispredictionsName = "SelectValuesLowerMispredictions_" + std::to_string(sizeof(T1))
            + "B_inputFilter_" + std::to_string(sizeof(T2)) + "B_inputData_" + std::to_string(dop) + "_dop";
    std::string machineConstantSelectivityName = "SelectValuesLowerSelectivity_" + std::to_string(sizeof(T1))
            + "B_inputFilter_" + std::to_string(sizeof(T2)) + "B_inputData_" + std::to_string(dop) + "_dop";
    MachineConstants::getInstance().updateMachineConstant(machineConstantMispredictionsName,
                                                          lowerMisPredictionsCrossoverPoints[NUMBER_OF_TESTS / 2]);
    MachineConstants::getInstance().updateMachineConstant(machineConstantSelectivityName,
                                                          lowerSelectivityCrossoverPoints[NUMBER_OF_TESTS / 2]);
}

template <typename T1, typename T2>
double calculateGroupByCrossoverCardinality(int dop) {
    int n = 40*1000*1000;

    long_long hashCycles, sortCycles;
    double upperCardinality = n;
    double lowerCardinality = 1;
    double midCardinality;

    std::string machineConstantName = "GroupBy_" + std::to_string(sizeof(T1)) + "B_inputFilter_" +
                                      std::to_string(sizeof(T2)) + "B_inputAggregate_" + std::to_string(dop) + "_dop";

    for (int i = 0; i < GROUPBY_ITERATIONS; ++i) {

        midCardinality = (lowerCardinality + upperCardinality) / 2;

        auto inputGroupBy = new T1[n];
        auto inputAggregate = new T2[n];
        generateUniformDistributionWithSetCardinalityInMemory(inputGroupBy, n, n, static_cast<int>(midCardinality));
        generateUniformDistributionInMemory(inputAggregate, n, 10);

        if (dop == 1) {
            hashCycles = *Counters::getInstance().readSharedEventSet();
            runGroupByFunction<MaxAggregation>(GroupBy::Hash, n, inputGroupBy, inputAggregate,
                                               static_cast<int>(midCardinality));
            hashCycles = *Counters::getInstance().readSharedEventSet() - hashCycles;
        } else {
            MachineConstants::getInstance().updateMachineConstant(machineConstantName, 0);
            hashCycles = PAPI_get_real_usec();
            runGroupByFunction<MaxAggregation>(GroupBy::AdaptiveParallel, n, inputGroupBy, inputAggregate,
                                               static_cast<int>(midCardinality), dop);
            hashCycles = PAPI_get_real_usec() - hashCycles;
        }

        delete[] inputGroupBy;
        delete[] inputAggregate;

        inputGroupBy = new T1[n];
        inputAggregate = new T2[n];
        generateUniformDistributionWithSetCardinalityInMemory(inputGroupBy, n, n, static_cast<int>(midCardinality));
        generateUniformDistributionInMemory(inputAggregate, n, 10);

        if (dop == 1) {
            sortCycles = *Counters::getInstance().readSharedEventSet();
            runGroupByFunction<MaxAggregation>(GroupBy::Sort, n, inputGroupBy, inputAggregate,
                                               static_cast<int>(midCardinality));
            sortCycles = *Counters::getInstance().readSharedEventSet() - sortCycles;
        } else {
            MachineConstants::getInstance().updateMachineConstant(machineConstantName, 1000000);
            sortCycles = PAPI_get_real_usec();
            runGroupByFunction<MaxAggregation>(GroupBy::AdaptiveParallel, n, inputGroupBy, inputAggregate,
                                               static_cast<int>(midCardinality), dop);
            sortCycles = PAPI_get_real_usec() - sortCycles;
        }

        delete[] inputGroupBy;
        delete[] inputAggregate;

        if (hashCycles > sortCycles) {
            upperCardinality = midCardinality;
        } else {
            lowerCardinality = midCardinality;
        }

//        std::cout << "Cardinality: " << (lowerCardinality + upperCardinality) / 2 << ", hash cycles: " << hashCycles << ", sort cycles: " << sortCycles << std::endl;
    }

    std::cout << ".";
    std::cout.flush();

    return (lowerCardinality + upperCardinality) / 2;
}

template<typename T1, typename T2>
struct GroupByHashCountLastLevelCacheMissesThreadArgs {
    int n;
    T1* inputGroupBy;
    T2* inputAggregate;
    int cardinality;
    long_long lastLevelCacheMisses;
};

template<typename T1, typename T2>
void *groupByHashCountLastLevelCacheMisses(void *arg) {
    auto* args = static_cast<GroupByHashCountLastLevelCacheMissesThreadArgs<T1, T2>*>(arg);
    int n = args->n;
    T1 *inputGroupBy = args->inputGroupBy;
    T2 *inputAggregate = args->inputAggregate;
    int cardinality = args->cardinality;

    tsl::robin_map<T1, T2> map(std::max(static_cast<int>(2.5 * cardinality), 400000));
    typename tsl::robin_map<T1, T2>::iterator it;

    int eventSet = PAPI_NULL;
    std::vector<std::string> counters = {"PERF_COUNT_HW_CACHE_MISSES"};
    long_long counterValues[1] = {0};
    createThreadEventSet(&eventSet, counters);

    readThreadEventSet(eventSet, 1, counterValues);
    for (int index = 0; index < n; ++index) {
        it = map.find(inputGroupBy[index]);
        if (it != map.end()) {
            it.value() = MaxAggregation<T2>()(it->second, inputAggregate[index], false);
        } else {
            map.insert({inputGroupBy[index], MaxAggregation<T2>()(0, inputAggregate[index], true)});
        }
    }
    readThreadEventSet(eventSet, 1, counterValues);
    args->lastLevelCacheMisses = counterValues[0];

    destroyThreadEventSet(eventSet, counterValues);
    return nullptr;
}


template <typename T1, typename T2>
double calculateGroupByMachineConstantsAuxParallel(int cardinality, int dop) {
    int n = 40*1000*1000;

    auto inputGroupBy = new T1[n];
    auto inputAggregate = new T2[n];
    generateUniformDistributionWithSetCardinalityInMemory(inputGroupBy, n, n, cardinality);
    generateUniformDistributionInMemory(inputAggregate, n, 10);

    Counters::getInstance();
    pthread_t threads[dop];
    std::vector<GroupByHashCountLastLevelCacheMissesThreadArgs<T1,T2>*> threadArgs(dop);

    std::vector<int> elementsPerThread(dop, n / dop);
    elementsPerThread[dop - 1] = n - ((dop - 1) * (n / dop));

    T1 *threadInputGroupBy = inputGroupBy;
    T2 *threadInputAggregate = inputAggregate;

    for (int i = 0; i < dop; ++i) {
        threadArgs[i] = new GroupByHashCountLastLevelCacheMissesThreadArgs<T1, T2>;
        threadArgs[i]->n = elementsPerThread[i];
        threadArgs[i]->inputGroupBy = threadInputGroupBy;
        threadArgs[i]->inputAggregate = threadInputAggregate;
        threadArgs[i]->cardinality = cardinality;

        pthread_create(&threads[i], NULL, groupByHashCountLastLevelCacheMisses<T1,T2>,
                       threadArgs[i]);

        threadInputGroupBy += elementsPerThread[i];
        threadInputAggregate += elementsPerThread[i];
    }

    for (int i = 0; i < dop; ++i) {
        pthread_join(threads[i], nullptr);
    }

    long_long totalLastLevelCacheMisses = 0;
    for (int i = 0; i < dop; ++i) {
        totalLastLevelCacheMisses += threadArgs[i]->lastLevelCacheMisses;
        delete threadArgs[i];
    }

    delete[] inputGroupBy;
    delete[] inputAggregate;

    std::cout << ".";
    std::cout.flush();

//    std::cout << "Tuples per last level cache miss: " << n / static_cast<double>(totalLastLevelCacheMisses) << std::endl;

    return  n / static_cast<double>(totalLastLevelCacheMisses);
}

template <typename T1, typename T2>
double calculateGroupByMachineConstantsAux(int cardinality) {
    int n = 40*1000*1000;

    std::vector<std::string> lastLevelCacheMissesCounter = {"PERF_COUNT_HW_CACHE_MISSES"};
    long_long *cacheMisses = Counters::getInstance().getSharedEventSetEvents(lastLevelCacheMissesCounter);

    auto inputGroupBy = new T1[n];
    auto inputAggregate = new T2[n];
    generateUniformDistributionWithSetCardinalityInMemory(inputGroupBy, n, n, cardinality);
    generateUniformDistributionInMemory(inputAggregate, n, 10);

    Counters::getInstance().readSharedEventSet();
    runGroupByFunction<MaxAggregation>(GroupBy::Hash, n, inputGroupBy, inputAggregate, cardinality);
    Counters::getInstance().readSharedEventSet();

    delete[] inputGroupBy;
    delete[] inputAggregate;

    std::cout << ".";
    std::cout.flush();

//    std::cout << "Tuples per last level cache miss: " << n / static_cast<double>(*cacheMisses) << std::endl;

    return  n / static_cast<double>(*cacheMisses);
}

template <typename T1, typename T2>
void calculateGroupByMachineConstants(int dop) {
    std::string machineConstantName = "GroupBy_" + std::to_string(sizeof(T1)) + "B_inputFilter_" +
            std::to_string(sizeof(T2)) + "B_inputAggregate_" + std::to_string(dop) + "_dop";
    std::cout << "Calculating machine constants for " << machineConstantName << std::endl;
    std::cout << " - Running tests for crossover point";
    std::cout.flush();

    std::vector<double> crossoverPoints;
    for (int i = 0; i < NUMBER_OF_TESTS; ++i) {
        crossoverPoints.push_back(calculateGroupByCrossoverCardinality<T1, T2>(dop));
    }
    std::sort(crossoverPoints.begin(), crossoverPoints.end());
    int crossoverCardinality = static_cast<int>(crossoverPoints[NUMBER_OF_TESTS / 2]);
    std::cout << " Complete" << std::endl;

    std::cout << " - Running tests for last level cache misses";
    std::vector<double> groupByMachineConstants;
    if (dop == 1) {
        for (int i = 0; i < NUMBER_OF_TESTS; ++i) {
            groupByMachineConstants.push_back(calculateGroupByMachineConstantsAux<T1, T2>(crossoverCardinality));
        }
    } else {
        for (int i = 0; i < NUMBER_OF_TESTS; ++i) {
            groupByMachineConstants.push_back(
                    calculateGroupByMachineConstantsAuxParallel<T1, T2>(crossoverCardinality, dop));
        }
    }
    std::sort(groupByMachineConstants.begin(), groupByMachineConstants.end());
    std::cout << " Complete" << std::endl;

    MachineConstants::getInstance().updateMachineConstant(machineConstantName,
                                                          groupByMachineConstants[NUMBER_OF_TESTS / 2]);
}


}


#endif //MABPL_MACHINECONSTANTSIMPLEMENTATION_H

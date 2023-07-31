#include <iostream>
#include <fstream>

#include "../mabpl.h"


namespace MABPL {

constexpr int NUMBER_OF_TESTS = 9;
constexpr int SELECT_ITERATIONS = 12;
constexpr int GROUPBY_ITERATIONS = 12;

// TO BE UPDATED - UPDATE TO FASTEST FILE FORMAT WITH KEY VALUE PAIRS THAT WE CAN READ IN
// TO WORK ON LAPTOP AND SERVER WITH NO CHANGES (RELATIVE FILE PATH?)
// NEED TO ENSURE THAT WE ONLY UPDATE PART OF FILE THAT NEEDS UPDATE - NOT DELETE WHOLE FILE
// NEED TO UPDATE ALL FUNCTIONS TO START BY READING VALUE FROM FILE AND THROWING AN ERROR IF NO VALUE IS SET
// MAKE FUNCTION TO SIMPLY GET VALUE BASED ON NAME
std::string MACHINE_CONSTANT_VALUES_FILE = "/home/jack/CLionProjects/micro-adaptive-bulk-processing-library/src/library/machine_constants/machineConstantValues.csv";


int getThreshold(int n, double selectivity) {
    return static_cast<int>(n * selectivity);
}

double calculateSelectIndexesLowerMachineConstant() {
    int n = 250*1000*1000;
    auto data = new int[n];
    generateRandomisedUniqueValuesInMemory(data, n);

    long_long branchCycles, predicationCycles;
    double upperSelectivity = 0.5;
    double lowerSelectivity = 0;
    double midSelectivity;

    for (int i = 0; i < SELECT_ITERATIONS; ++i) {

        midSelectivity = (lowerSelectivity + upperSelectivity) / 2;

        auto inputFilter = new int[n];
        auto selection = new int[n];
        copyArray(data, inputFilter, n);

        branchCycles = *Counters::getInstance().readSharedEventSet();
        runSelectFunction(Select::ImplementationIndexesBranch, n, inputFilter, inputFilter,
                          selection, getThreshold(n, midSelectivity));
        branchCycles = *Counters::getInstance().readSharedEventSet() - branchCycles;

        delete[] inputFilter;
        delete[] selection;

        inputFilter = new int[n];
        selection = new int[n];
        copyArray(data, inputFilter, n);

        predicationCycles = *Counters::getInstance().readSharedEventSet();
        runSelectFunction(Select::ImplementationIndexesPredication, n, inputFilter, inputFilter,
                          selection, getThreshold(n, midSelectivity));
        predicationCycles = *Counters::getInstance().readSharedEventSet() - predicationCycles;

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

double calculateSelectIndexesUpperMachineConstant() {
    int n = 250*1000*1000;
    auto data = new int[n];
    generateRandomisedUniqueValuesInMemory(data, n);

    long_long branchCycles, predicationCycles;
    double upperSelectivity = 1.0;
    double lowerSelectivity = 0.5;
    double midSelectivity;

    for (int i = 0; i < SELECT_ITERATIONS; ++i) {

        midSelectivity = (lowerSelectivity + upperSelectivity) / 2;

        auto inputFilter = new int[n];
        auto selection = new int[n];
        copyArray(data, inputFilter, n);

        branchCycles = *Counters::getInstance().readSharedEventSet();
        runSelectFunction(Select::ImplementationIndexesBranch, n, inputFilter, inputFilter,
                          selection, getThreshold(n, midSelectivity));
        branchCycles = *Counters::getInstance().readSharedEventSet() - branchCycles;

        delete[] inputFilter;
        delete[] selection;

        inputFilter = new int[n];
        selection = new int[n];
        copyArray(data, inputFilter, n);

        predicationCycles = *Counters::getInstance().readSharedEventSet();
        runSelectFunction(Select::ImplementationIndexesPredication, n, inputFilter, inputFilter,
                          selection, getThreshold(n, midSelectivity));
        predicationCycles = *Counters::getInstance().readSharedEventSet() - predicationCycles;

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

void calculateSelectIndexesMachineConstants() {
    std::cout << "Calculating machine constants for SelectIndexes" << std::endl;
    std::cout << " - Running tests for lower crossover point";

    std::vector<double> lowerCrossoverPoints;
    for (int i = 0; i < NUMBER_OF_TESTS; ++i) {
        lowerCrossoverPoints.push_back(calculateSelectIndexesLowerMachineConstant());
    }
    std::sort(lowerCrossoverPoints.begin(), lowerCrossoverPoints.end());
    std::cout << " Complete" << std::endl;

    std::cout << " - Running tests for upper crossover point";
    std::vector<double> upperCrossoverPoints;
    for (int i = 0; i < NUMBER_OF_TESTS; ++i) {
        upperCrossoverPoints.push_back(calculateSelectIndexesUpperMachineConstant());
    }
    std::sort(upperCrossoverPoints.begin(), upperCrossoverPoints.end());
    std::cout << " Complete" << std::endl;

    std::ofstream file(MACHINE_CONSTANT_VALUES_FILE);
    if (file.is_open()) {
        file << "SelectIndexesLower," << lowerCrossoverPoints[NUMBER_OF_TESTS / 2] << "\n";
        file << "SelectIndexesUpper," << upperCrossoverPoints[NUMBER_OF_TESTS / 2] << "\n";
    } else {
        std::cerr << "Failed to open file: " << MACHINE_CONSTANT_VALUES_FILE << "\n";
    }
    file.close();
}

double calculateSelectValuesLowerMachineConstant() {
    int n = 250*1000*1000;
    auto data = new int[n];
    generateRandomisedUniqueValuesInMemory(data, n);

    long_long branchCycles, vectorizationCycles;
    double upperSelectivity = 0.5;
    double lowerSelectivity = 0;
    double midSelectivity;

    for (int i = 0; i < SELECT_ITERATIONS; ++i) {

        midSelectivity = (lowerSelectivity + upperSelectivity) / 2;

        auto inputFilter = new int[n];
        auto inputData = new int[n];
        auto selection = new int[n];
        copyArray(data, inputFilter, n);
        copyArray(data, inputData, n);

        branchCycles = *Counters::getInstance().readSharedEventSet();
        runSelectFunction(Select::ImplementationValuesBranch, n, inputData, inputFilter,
                          selection, getThreshold(n, midSelectivity));
        branchCycles = *Counters::getInstance().readSharedEventSet() - branchCycles;

        delete[] inputFilter;
        delete[] inputData;
        delete[] selection;

        inputFilter = new int[n];
        inputData = new int[n];;
        selection = new int[n];
        copyArray(data, inputFilter, n);
        copyArray(data, inputData, n);

        vectorizationCycles = *Counters::getInstance().readSharedEventSet();
        runSelectFunction(Select::ImplementationValuesVectorized, n, inputData, inputFilter,
                          selection, getThreshold(n, midSelectivity));
        vectorizationCycles = *Counters::getInstance().readSharedEventSet() - vectorizationCycles;

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

    delete[] data;

    return (upperSelectivity + lowerSelectivity) / 2;
}

void calculateSelectValuesMachineConstants() {
    std::cout << "Calculating machine constants for SelectValues" << std::endl;
    std::cout << " - Running tests for lower crossover point";

    std::vector<double> lowerCrossoverPoints;
    for (int i = 0; i < NUMBER_OF_TESTS; ++i) {
        lowerCrossoverPoints.push_back(calculateSelectValuesLowerMachineConstant());
    }
    std::sort(lowerCrossoverPoints.begin(), lowerCrossoverPoints.end());
    std::cout << " Complete" << std::endl;

    std::ofstream file(MACHINE_CONSTANT_VALUES_FILE);
    if (file.is_open()) {
        file << "SelectValuesLower," << lowerCrossoverPoints[NUMBER_OF_TESTS / 2] << "\n";
    } else {
        std::cerr << "Failed to open file: " << MACHINE_CONSTANT_VALUES_FILE << "\n";
    }
    file.close();
}


double calculateGroupByCrossoverCardinality() {
    int n = 40*1000*1000;

    long_long hashCycles, sortCycles;
    double upperCardinality = n;
    double lowerCardinality = 1;
    double midCardinality;

    for (int i = 0; i < GROUPBY_ITERATIONS; ++i) {

        midCardinality = (lowerCardinality + upperCardinality) / 2;

        auto inputGroupBy = new int[n];
        auto inputAggregate = new int[n];
        generateUniformDistributionWithSetCardinalityInMemory(inputGroupBy, n, n, static_cast<int>(midCardinality));
        generateUniformDistributionInMemory(inputAggregate, n, 10);

        hashCycles = *Counters::getInstance().readSharedEventSet();
        runGroupByFunction<MaxAggregation>(GroupBy::Hash, n, inputGroupBy, inputAggregate,
                                           static_cast<int>(midCardinality));
        hashCycles = *Counters::getInstance().readSharedEventSet() - hashCycles;

        delete[] inputGroupBy;
        delete[] inputAggregate;

        inputGroupBy = new int[n];
        inputAggregate = new int[n];
        generateUniformDistributionWithSetCardinalityInMemory(inputGroupBy, n, n, static_cast<int>(midCardinality));
        generateUniformDistributionInMemory(inputAggregate, n, 10);

        sortCycles = *Counters::getInstance().readSharedEventSet();
        runGroupByFunction<MaxAggregation>(GroupBy::Sort, n, inputGroupBy, inputAggregate,
                                           static_cast<int>(midCardinality));
        sortCycles = *Counters::getInstance().readSharedEventSet() - sortCycles;

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

double calculateGroupByMachineConstantsAux(int cardinality) {
    int n = 40*1000*1000;

    std::vector<std::string> lastLevelCacheMissesCounter = {"PERF_COUNT_HW_CACHE_MISSES"};
    long_long *cacheMisses = Counters::getInstance().getSharedEventSetEvents(lastLevelCacheMissesCounter);

    auto inputGroupBy = new int[n];
    auto inputAggregate = new int[n];
    generateUniformDistributionWithSetCardinalityInMemory(inputGroupBy, n, n, cardinality);
    generateUniformDistributionInMemory(inputAggregate, n, 10);

    Counters::getInstance().readSharedEventSet();
    runGroupByFunction<MaxAggregation>(GroupBy::Hash, n, inputGroupBy, inputAggregate, cardinality);
    Counters::getInstance().readSharedEventSet();

    delete[] inputGroupBy;
    delete[] inputAggregate;

    std::cout << ".";
    std::cout.flush();

    std::cout << "Tuples per last level cache miss: " << n / static_cast<double>(*cacheMisses) << std::endl;

    return  n / static_cast<double>(*cacheMisses);
}

void calculateGroupByMachineConstants() {
    std::cout << "Calculating machine constants for GroupBy" << std::endl;
    std::cout << " - Running tests for crossover point";

    std::vector<double> crossoverPoints;
    for (int i = 0; i < NUMBER_OF_TESTS; ++i) {
        crossoverPoints.push_back(calculateGroupByCrossoverCardinality());
    }
    std::sort(crossoverPoints.begin(), crossoverPoints.end());
    int crossoverCardinality = static_cast<int>(crossoverPoints[NUMBER_OF_TESTS / 2]);
    std::cout << " Complete" << std::endl;

//    int crossoverCardinality = 629884;

    std::cout << " - Running tests for last level cache misses";
    std::vector<double> groupByMachineConstants;
    for (int i = 0; i < NUMBER_OF_TESTS; ++i) {
        groupByMachineConstants.push_back(calculateGroupByMachineConstantsAux(crossoverCardinality));
    }
    std::sort(groupByMachineConstants.begin(), groupByMachineConstants.end());
    std::cout << " Complete" << std::endl;

    std::ofstream file(MACHINE_CONSTANT_VALUES_FILE);
    if (file.is_open()) {
        file << "GroupBy," << groupByMachineConstants[NUMBER_OF_TESTS / 2] << "\n";
    } else {
        std::cerr << "Failed to open file: " << MACHINE_CONSTANT_VALUES_FILE << "\n";
    }
    file.close();
}

void calculateMissingMachineConstants() {
    // Read file in and go through each key value pair and only run function if key value pair is missing in file
//    calculateSelectIndexesMachineConstants();
//    calculateSelectValuesMachineConstants();
    calculateGroupByMachineConstants();
}

void clearAndRecalculateMachineConstants() {
    // Clear all machine constants
    calculateSelectIndexesMachineConstants();
    calculateSelectValuesMachineConstants();
    calculateGroupByMachineConstants();
}

void printMachineConstants() {
    // PRINT CALL MACHINE CONSTANTS FROM FILE
    std::cout << "Machine constants not calculated yet!" << std::endl;
}


}

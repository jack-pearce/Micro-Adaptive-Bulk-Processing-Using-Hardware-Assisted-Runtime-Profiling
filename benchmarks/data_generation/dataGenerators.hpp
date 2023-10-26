#ifndef MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATA_GENERATOR_H
#define MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATA_GENERATOR_H

#include "dataFiles.hpp"

std::vector<int> generateClusteringOrder(int n, int spreadInCluster);

template <typename T>
class LoadedData {
private:
    T *data;
    const DataFile *dataFile;
    explicit LoadedData(const DataFile &dataFile);
    void loadData();

public:
    static LoadedData &getInstance(const DataFile &requestedDataFile);
    [[nodiscard]] T* getData() const;
    [[nodiscard]] const DataFile& getDataFile() const;
    LoadedData(const LoadedData&) = delete;
    void operator=(const LoadedData&) = delete;
};

template <typename T>
void generateVaryingSelectivityInMemory(T *data, int n, int minimum, int numberOfDiscreteSections);

template <typename T>
void generateUpperStepSelectivityInMemory(T *data, int n, int step, int numberOfDiscreteSections);

template <typename T>
void generateLowerStepSelectivityInMemory(T *data, int n, int step, int numberOfDiscreteSections);

template <typename T>
void generateLowerStepSelectivityInMemoryLengthOfSection(T *data, int n, int step, int sectionLength);

template <typename T>
void generateUnequalLowerStepSelectivityInMemory(T *data, int n, int step, int numberOfDiscreteSections,
                                                 float percentStepSection);

template <typename T>
void generatePartiallySortedInMemoryOneToOneHundred(T *data, int n, int numRepeats, float percentageRandom);

template <typename T>
void generateFullySortedUniqueInMemory(T *data, int n);

template <typename T>
void generatePartiallySortedUniqueInMemory(T *data, int n, float percentageRandom);

template <typename T>
void generateUniformDistributionInMemory(T *data, int n, int upperBound);

template <typename T>
inline T scaleNumberLinearly(int number, int startingUpperBound, int targetUpperBound);

template <typename T>
inline T scaleNumberLogarithmically(T number, int startingUpperBound, int targetUpperBound);

template <typename T>
void generateUniqueValuesRandomisedInMemory(T *data, int n);

template <typename T>
void generateUniformDistributionInMemoryWithSetCardinality(T *data, int n, int upperBound, int cardinality);

template <typename T>
void generateUniformDistributionInMemoryWithSetCardinalityClustered(T *data, int n, int upperBound,
                                                                    int cardinality, int spreadInCluster);

template <typename T>
void generateUniformDistributionInMemoryWithSetCardinalityClusteredAlternative(T *data, int n, int upperBound,
                                                                               int cardinality, int spreadInCluster);
template <typename T>
void generateUniformDistributionInMemoryWithTwoCardinalitySections(T *data, int n, int upperBound,
                                                                   int cardinalitySectionOne, int cardinalitySectionTwo,
                                                                   float fractionSectionTwo);

template <typename T>
void generateUniformDistributionInMemoryWithMultipleTwoCardinalitySections(T *data, int n, int upperBound,
                                                                           int cardinalitySectionOne,
                                                                           int cardinalitySectionTwo,
                                                                           float fractionSectionTwo,
                                                                           int numSections);

template <typename T>
void runClusteringOnData(T *data, int n, int spreadInCluster);


#include "dataGeneratorsImplementation.hpp"

#endif //MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATA_GENERATOR_H

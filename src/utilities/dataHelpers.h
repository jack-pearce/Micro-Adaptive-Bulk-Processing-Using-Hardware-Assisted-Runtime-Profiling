#ifndef MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATAHELPERS_H
#define MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATAHELPERS_H

#include <algorithm>

#include "../../libs/papi/src/install/include/papi.h"
#include "../data_generation/dataFiles.h"
#include "../library/mabpl.h"

class LoadedData {
private:
    int *data;
    const DataFile *dataFile;
    explicit LoadedData(const DataFile &dataFile);
    void loadData();

public:
    static LoadedData &getInstance(const DataFile &requestedDataFile);
    [[nodiscard]] int* getData() const;
    [[nodiscard]] const DataFile& getDataFile() const;
    LoadedData(const LoadedData&) = delete;
    void operator=(const LoadedData&) = delete;
};

void generateEvenIntLogDistribution(int numPoints, double minValue, double maxValue, std::vector<float> &points);
void generateLogDistribution(int numPoints, double minValue, double maxValue, std::vector<float> &points);
void generateLinearDistribution(int numPoints, double minValue, double maxValue, std::vector<float> &points);
void displayDistribution(const DataFile &dataFile);
void writeDataFileToCSV(const DataFile &dataFile);
void writeHeadersAndTableToCSV(std::vector<std::string>& headers,
                               std::vector<std::vector<long_long>>  values,
                               std::string& filePath);
void writeHeadersAndTableToCSV(std::vector<std::string>& headers,
                               std::vector<std::vector<double>>  values,
                               std::string& filePath);
void copyArray(const int* source, int* destination, int size);

template <typename T1, typename T2>
void sortVectorOfPairs(MABPL::vectorOfPairs<T1, T2> &vectorOfPairs) {;
    std::sort(vectorOfPairs.begin(), vectorOfPairs.end(), [](const auto& pair1, const auto& pair2) {
        return pair1.first < pair2.first;
    });
}


#endif //MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATAHELPERS_H

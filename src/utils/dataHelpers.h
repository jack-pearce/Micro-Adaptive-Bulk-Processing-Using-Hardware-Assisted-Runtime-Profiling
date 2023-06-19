#ifndef MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATAHELPERS_H
#define MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATAHELPERS_H

#include <memory>

#include "../../libs/papi/src/install/include/papi.h"
#include "../data_generation/dataFiles.h"

class LoadedData {
private:
    std::unique_ptr<int[]> data;
    const DataFile &dataFile;
    explicit LoadedData(const DataFile &dataFile);

public:
    static LoadedData &getInstance(const DataFile &dataFile);
    [[nodiscard]] int* getData() const;
    [[nodiscard]] const DataFile& getDataFile() const;
    LoadedData(const LoadedData&) = delete;
    void operator=(const LoadedData&) = delete;
};

void generateLogDistribution(int numPoints, double minValue, double maxValue, std::vector<float> &points);
void generateLinearDistribution(int numPoints, double minValue, double maxValue, std::vector<float> &points);
void displayDistribution(const DataFile& dataFile);
void writeDataFileToCSV(const DataFile& dataFile);
void writeHeadersAndTableToCSV(std::vector<std::string>& headers,
                               std::vector<std::vector<long_long>>  values,
                               std::string& filePath);
void writeHeadersAndTableToCSV(std::vector<std::string>& headers,
                               std::vector<std::vector<double>>  values,
                               std::string& filePath);
void copyArray(const int* source, int* destination, int size);

#endif //MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATAHELPERS_H

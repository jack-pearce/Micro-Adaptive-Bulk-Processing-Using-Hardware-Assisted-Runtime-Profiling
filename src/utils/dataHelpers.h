#ifndef MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATAHELPERS_H
#define MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATAHELPERS_H

#include <memory>

#include "../../libs/papi/src/install/include/papi.h"
#include "../data_generation/dataFiles.h"

class LoadedData {
private:
    std::unique_ptr<int[]> data;
    int size;

    LoadedData(const std::string& filePath, int numElements);

public:
    static LoadedData& getInstance(const std::string& filePath, int numElements);

    int* getData();
    int getSize() const;

    LoadedData(const LoadedData&) = delete;
    void operator=(const LoadedData&) = delete;
};

int loadDataToArray(const std::string& filePath, int *data);
void displayDistribution(const DataFile& dataFile);
void writeHeadersAndTableToCSV(std::vector<std::string>& headers,
                               std::vector<std::vector<long_long>>  values,
                               std::string& filePath);

#endif //MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATAHELPERS_H

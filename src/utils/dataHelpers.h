#ifndef MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATAHELPERS_H
#define MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATAHELPERS_H

#include "../../libs/papi/src/install/include/papi.h"

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

int countElements(const std::string& filePath);
void loadDataToVector(const std::string& filePath, std::vector<int>& data);
int loadDataToArray(const std::string& filePath, int *data);
void displayDistribution(const std::vector<int>& data);
void writeHeadersAndTableToCSV(std::vector<std::string>& headers,
                               std::vector<std::vector<long_long>>  values,
                               std::string& filePath);

#endif //MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATAHELPERS_H

#ifndef MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATAHELPERS_H
#define MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATAHELPERS_H

#include <algorithm>

#include "../../libs/papi/src/install/include/papi.h"
#include "../data_generation/dataFiles.h"
#include "../library/mabpl.h"


void writeHeadersAndTableToCSV(std::vector<std::string>& headers,
                               std::vector<std::vector<long_long>>  values,
                               std::string& filePath);

void writeHeadersAndTableToCSV(std::vector<std::string>& headers,
                               std::vector<std::vector<double>>  values,
                               std::string& filePath);


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
void displayDistribution(const DataFile &dataFile);

template <typename T>
void writeDataFileToCSV(const DataFile &dataFile);

template <typename T1, typename T2>
void sortVectorOfPairs(MABPL::vectorOfPairs<T1, T2> &vectorOfPairs);

template <typename T>
void copyArray(T* source, T* destination, int size);

#include "dataHelpersImplementation.h"


#endif //MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATAHELPERS_H

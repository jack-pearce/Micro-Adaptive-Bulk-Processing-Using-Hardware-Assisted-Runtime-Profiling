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

int getLengthOfCsv(const std::string &filePath);
int getLengthOfTsv(const std::string& filePath);

void readImdbTitleIdColumnFromBasicsTable(const std::string& filePath, uint32_t* data);
void readImdbStartYearColumnFromBasicsTable(const std::string& filePath, int* data);
void readImdbStartYearColumnFromBasicsTable(const std::string& filePath, uint32_t* data);

void readImdbTitleIdColumnFromPrincipalsTable(const std::string& filePath, uint32_t* data);
void readImdbPersonIdColumnFromPrincipalsTable(const std::string& filePath, uint32_t* data);

void readImdbTitleIdColumnFromAkasTable(const std::string& filePath, int* data);
void readImdbTitleIdColumnFromAkasTable(const std::string& filePath, int64_t* data);

template <typename T>
void displayDistribution(const DataFile &dataFile);

template <typename T>
void writeDataFileToCSV(const DataFile &dataFile);

template <typename T1, typename T2>
void sortVectorOfPairs(MABPL::vectorOfPairs<T1, T2> &vectorOfPairs);

template <typename T>
void copyArray(T* source, T* destination, int size);

template <typename T>
void projectIndexesOnToArray(const int* indexes, int n, T* source, T* destination);

template <typename T>
void randomiseArray(T* data, int n);

#include "dataHelpersImplementation.h"


#endif //MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATAHELPERS_H

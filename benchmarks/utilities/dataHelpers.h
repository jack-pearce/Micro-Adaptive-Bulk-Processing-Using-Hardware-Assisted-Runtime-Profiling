#ifndef MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATAHELPERS_H
#define MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATAHELPERS_H

#include <algorithm>
#include <papi.h>

#include "haqp.h"
#include "data_generation/dataFiles.h"


int getLengthOfFile(const std::string& filePath);


template <typename T>
void writeHeadersAndTableToCSV(std::vector<std::string>& headers,
                               std::vector<std::vector<T>>  values,
                               std::string& filePath);

template <typename T>
void readImdbTitleIdColumnFromBasicsTable(const std::string& filePath, T* data);

template <typename T>
void readImdbStartYearColumnFromBasicsTable(const std::string& filePath, T* data);

template <typename T>
void readImdbTitleIdColumnFromPrincipalsTable(const std::string& filePath, T* data);

template <typename T>
void readImdbPersonIdColumnFromPrincipalsTable(const std::string& filePath, T* data);

template <typename T>
void readImdbTitleIdColumnFromAkasTable(const std::string& filePath, T* data);

template <typename T>
void displayDistribution(const DataFile &dataFile);

template <typename T>
void writeDataFileToCSV(const DataFile &dataFile);

template <typename T1, typename T2>
void sortVectorOfPairs(HAQP::vectorOfPairs<T1, T2> &vectorOfPairs);

template <typename T>
void copyArray(T* source, T* destination, int size);

template <typename T>
void projectIndexesOnToArray(const int* indexes, int n, T* source, T* destination);

template <typename T>
void randomiseArray(T* data, int n);

#include "dataHelpersImplementation.h"


#endif //MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATAHELPERS_H

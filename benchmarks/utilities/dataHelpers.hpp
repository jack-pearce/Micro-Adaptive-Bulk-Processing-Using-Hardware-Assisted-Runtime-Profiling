#ifndef MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATAHELPERS_H
#define MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATAHELPERS_H

#include <algorithm>
#include <papi.h>

#include "haqp.hpp"
#include "data_generation/dataFiles.hpp"


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
void copyArray(T* source, T* destination, int size);


#include "dataHelpersImplementation.hpp"


#endif //MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATAHELPERS_H

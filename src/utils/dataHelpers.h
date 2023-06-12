#ifndef MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATAHELPERS_H
#define MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATAHELPERS_H

#include "../../libs/papi/src/install/include/papi.h"

int countElements(const std::string& filePath);
void loadDataToVector(const std::string& filePath, std::vector<int>& data);
int loadDataToArray(const std::string& filePath, int *data);
void displayDistribution(const std::vector<int>& data);
void writeHeadersAndTableToCSV(std::vector<std::string>& headers,
                               std::vector<std::vector<long_long>>  values,
                               std::string& filePath);

#endif //MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATAHELPERS_H

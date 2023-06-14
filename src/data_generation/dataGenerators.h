#ifndef MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATA_GENERATOR_H
#define MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATA_GENERATOR_H

void generateUniformDistributionCSV(const std::string& fileName, int n);
void generateVaryingSelectivityCSV(const std::string& fileName, int n, int minimum);
void generateStepSelectivityCSV(const std::string& fileName, int n, int step);

#endif //MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATA_GENERATOR_H

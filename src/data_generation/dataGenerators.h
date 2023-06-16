#ifndef MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATA_GENERATOR_H
#define MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATA_GENERATOR_H

void generateUniformDistributionInMemory(int *data, int n);
void generateVaryingSelectivityInMemory(int *data, int n, int minimum, int numberOfDiscreteSections);
void generateStepSelectivityInMemory(int *data, int n, int step, int numberOfDiscreteSections);
void generateUnequalStepSelectivityInMemory(int *data, int n, int step, int numberOfDiscreteSections, int sectionRatio);
void generatePartiallySortedInMemory(int *data, int n, int numRepeats, float percentageRandom);

#endif //MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATA_GENERATOR_H

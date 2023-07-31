#ifndef MABPL_DATAGENERATION_H
#define MABPL_DATAGENERATION_H

namespace MABPL {

void copyArray(const int* source, int* destination, int size);
void generateUniformDistributionInMemory(int *data, int n, int upperBound);
void generateRandomisedUniqueValuesInMemory(int *data, int n);
void generateUniformDistributionWithSetCardinalityInMemory(int *data, int n, int upperBound, int cardinality);

}

#endif //MABPL_DATAGENERATION_H

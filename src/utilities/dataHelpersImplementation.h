#ifndef MABPL_DATAHELPERSIMPLEMENTATION_H
#define MABPL_DATAHELPERSIMPLEMENTATION_H

#include <fstream>
#include <string>
#include <sstream>
#include <algorithm>
#include <random>


template <typename T>
void displayDistribution(const DataFile &dataFile) {
    std::vector<int> counts(101, 0);
    auto numElements = dataFile.getNumElements();
    T *inputData = LoadedData<T>::getInstance(dataFile).getData();
    for (auto i = 0; i < numElements; ++i) {
        counts[inputData[i]]++;
    }
    for (auto i = 0; i <= 100; ++i) {
        std::cout << i << "%: " << static_cast<float>(counts[i]) / static_cast<float>(numElements) << std::endl;
    }
}

template <typename T>
void writeDataFileToCSV(const DataFile &dataFile) {
    T *data = LoadedData<T>::getInstance(dataFile).getData();
    auto numElements = dataFile.getNumElements();

    std::string fileName = FilePaths::getInstance().getDataFilesFolderPath() + dataFile.getFileName() + ".csv";
    std::ofstream file(fileName);

    if (file.is_open()) {
        for (auto i = 0; i < numElements; ++i) {
            file << data[i];
            if (i < numElements - 1) {
                file << ",\n";
            }
        }

        file.close();
        std::cout << "CSV file created successfully: " << fileName << std::endl;
    } else {
        std::cout << "Unable to create the CSV file: " << fileName << std::endl;
    }
}

template <typename T1, typename T2>
void sortVectorOfPairs(MABPL::vectorOfPairs<T1, T2> &vectorOfPairs) {;
    std::sort(vectorOfPairs.begin(), vectorOfPairs.end(), [](const auto& pair1, const auto& pair2) {
        return pair1.first < pair2.first;
    });
}

template <typename T>
void copyArray(T* source, T* destination, int size) {
    for (auto i = 0; i < size; i++) {
        destination[i] = source[i];
    }
}

template <typename T>
void projectIndexesOnToArray(const int* indexes, int n, T* source, T* destination) {
    for (int i = 0; i < n; i++) {
        destination[i] = source[indexes[i]];
    }
}

template <typename T>
void randomiseArray(T* data, int n) {
    unsigned int seed = 1;
    std::mt19937 gen(seed);
    std::shuffle(data, data + n, gen);
}


#endif //MABPL_DATAHELPERSIMPLEMENTATION_H

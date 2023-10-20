#ifndef HAQP_DATAHELPERSIMPLEMENTATION_H
#define HAQP_DATAHELPERSIMPLEMENTATION_H

#include <fstream>
#include <string>
#include <sstream>
#include <algorithm>
#include <random>

template <typename T>
void writeHeadersAndTableToCSV(std::vector<std::string>& headers,
                               std::vector<std::vector<T>>  values,
                               std::string& filePath) {
    assert(headers.size() == values[0].size());

    std::ofstream file(filePath);

    for (size_t i = 0; i < headers.size(); ++i) {
        file << headers[i];
        if (i != headers.size() - 1) {
            file << ",";
        }
    }
    file << "\n";

    if (file.is_open()) {
        for (const auto& row : values) {
            for (size_t i = 0; i < row.size(); ++i) {
                file << row[i];
                if (i != row.size() - 1) {
                    file << ",";
                }
            }
            file << "\n";
        }
        file.close();
    } else {
        std::cerr << "Failed to open file: " << filePath << "\n";
    }
}

template <typename T>
void readImdbTitleIdColumnFromBasicsTable(const std::string& filePath, T* data) {
    static_assert(std::is_integral<T>::value, "Must be an integer type");
    std::ifstream file(filePath);
    if (!file.is_open()) {
        std::cerr << "Error opening file: " << filePath << std::endl;
        exit(1);
    }

    std::string line;
    int index = 0;
    bool isFirstRow = true;
    while (std::getline(file, line)) {
        if (isFirstRow) {
            isFirstRow = false;
            continue;
        }

        std::istringstream iss(line);
        std::string filmId, filmNumOnly;

        std::getline(iss, filmId, '\t');
        filmNumOnly = filmId.substr(2);
        data[index++] = std::stoi(filmNumOnly);
    }

    file.close();
}

template <typename T>
void readImdbStartYearColumnFromBasicsTable(const std::string& filePath, T* data) {
    static_assert(std::is_integral<T>::value, "Must be an integer type");
    std::ifstream file(filePath);
    if (!file.is_open()) {
        std::cerr << "Error opening file: " << filePath << std::endl;
        exit(1);
    }

    std::string line;
    int index = 0;
    bool isFirstRow = true;
    while (std::getline(file, line)) {
        if (isFirstRow) {
            isFirstRow = false;
            continue;
        }

        std::istringstream iss(line);
        std::string value;
        for (size_t i = 0; i < 6; ++i) {
            std::getline(iss, value, '\t');
        }

        try {
            data[index] = std::stoi(value);
        } catch (const std::invalid_argument &e) {
            if (std::is_same<T, int>::value) {
                data[index] = 9999; // To ensure that null values are not picked up by if statement
            } else {
                data[index] = 0; // Standard null representation of year
            }

        }
        index++;
    }

    file.close();
}

template <typename T>
void readImdbTitleIdColumnFromPrincipalsTable(const std::string& filePath, T* data) {
    static_assert(std::is_integral<T>::value, "Must be an integer type");
    std::ifstream file(filePath);
    if (!file.is_open()) {
        std::cerr << "Error opening file: " << filePath << std::endl;
        exit(1);
    }

    std::string line;
    int index = 0;
    bool isFirstRow = true;
    while (std::getline(file, line)) {
        if (isFirstRow) {
            isFirstRow = false;
            continue;
        }

        std::istringstream iss(line);
        std::string titleId, titleNumOnly;

        std::getline(iss, titleId, '\t');
        titleNumOnly = titleId.substr(2);
        data[index++] = std::stoi(titleNumOnly);
    }

    file.close();
}

template <typename T>
void readImdbPersonIdColumnFromPrincipalsTable(const std::string& filePath, T* data) {
    static_assert(std::is_integral<T>::value, "Must be an integer type");
    std::ifstream file(filePath);
    if (!file.is_open()) {
        std::cerr << "Error opening file: " << filePath << std::endl;
        exit(1);
    }

    std::string line;
    int index = 0;
    bool isFirstRow = true;
    while (std::getline(file, line)) {
        if (isFirstRow) {
            isFirstRow = false;
            continue;
        }

        std::istringstream iss(line);
        std::string dummy1, dummy2, personId, personNumOnly;

        std::getline(iss, dummy1, '\t');
        std::getline(iss, dummy2, '\t');
        std::getline(iss, personId, '\t');

        personNumOnly = personId.substr(2);
        data[index++] = std::stoi(personNumOnly);
    }

    file.close();
}

template <typename T>
void readImdbTitleIdColumnFromAkasTable(const std::string& filePath, T* data) {
    static_assert(std::is_integral<T>::value, "Must be an integer type");
    std::ifstream file(filePath);
    if (!file.is_open()) {
        std::cerr << "Error opening file: " << filePath << std::endl;
        exit(1);
    }

    std::string line;
    int index = 0;
    bool isFirstRow = true;
    while (std::getline(file, line)) {
        if (isFirstRow) {
            isFirstRow = false;
            continue;
        }

        std::istringstream iss(line);
        std::string filmId, filmNumOnly;

        std::getline(iss, filmId, '\t');
        filmNumOnly = filmId.substr(2);
        data[index++] = std::stoi(filmNumOnly);
    }

    file.close();
}

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
void sortVectorOfPairs(HAQP::vectorOfPairs<T1, T2> &vectorOfPairs) {;
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

template <typename T>
void dataDistributionTest(const DataFile& dataFile) {
    LoadedData<T>::getInstance(dataFile);
    std::cout << dataFile.getNumElements() << " elements" << std::endl;
    displayDistribution<T>(dataFile);
}


#endif //HAQP_DATAHELPERSIMPLEMENTATION_H

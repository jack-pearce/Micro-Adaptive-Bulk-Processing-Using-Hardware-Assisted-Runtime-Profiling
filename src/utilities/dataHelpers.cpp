#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <cassert>
#include <stdexcept>
#include <cmath>

#include "dataHelpers.h"


void writeHeadersAndTableToCSV(std::vector<std::string>& headers,
                               std::vector<std::vector<long_long>>  values,
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

void writeHeadersAndTableToCSV(std::vector<std::string>& headers,
                               std::vector<std::vector<double>>  values,
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

int getLengthOfCsv(const std::string &filePath) {
    std::ifstream file(filePath);
    if (!file.is_open()) {
        std::cerr << "Error opening file: " << filePath << std::endl;
        exit(1);
    }

    int rowCount = 0;
    std::string line;
    while (std::getline(file, line)) {
        rowCount++;
    }

    file.close();
    return rowCount - 1; // Assumes that the first row is used for column headers
}

int getLengthOfTsv(const std::string& filePath) {
    int rowCount = 0;

    std::ifstream file(filePath);
    if (!file.is_open()) {
        std::cerr << "Error opening file: " << filePath << std::endl;
        exit(1);
    }

    std::string line;
    while (std::getline(file, line)) {
        rowCount++;
    }

    file.close();
    return rowCount - 1; // Assumes that the first row is used for column headers
}

void readImdbTitleIdColumnFromBasicsTable(const std::string& filePath, uint32_t* data) {
    std::ifstream file(filePath);
    if (!file.is_open()) {
        std::cerr << "Error opening file: " << filePath << std::endl;
        exit(1);
    }

    std::string line;
    int index = 0;
    bool isFirstRow = true;  // Flag to skip the first row
    while (std::getline(file, line)) {
        if (isFirstRow) {
            isFirstRow = false;
            continue;  // Skip the first row
        }

        std::istringstream iss(line);
        std::string filmId, filmNumOnly;

        std::getline(iss, filmId, '\t');
        filmNumOnly = filmId.substr(2);
        data[index++] = std::stoi(filmNumOnly);
    }

    file.close();
}

void readImdbStartYearColumnFromBasicsTable(const std::string& filePath, int* data) {
    std::ifstream file(filePath);
    if (!file.is_open()) {
        std::cerr << "Error opening file: " << filePath << std::endl;
        exit(1);
    }

    std::string line;
    int index = 0;
    bool isFirstRow = true;  // Flag to skip the first row
    while (std::getline(file, line)) {
        if (isFirstRow) {
            isFirstRow = false;
            continue;  // Skip the first row
        }

        std::istringstream iss(line);
        std::string value;
        for (size_t i = 0; i < 6; ++i) {
            std::getline(iss, value, '\t');
        }

        try {
            data[index] = std::stoi(value);
        } catch (const std::invalid_argument &e) {
            data[index] = 9999;
        }
        index++;
    }

    file.close();
}

void readImdbStartYearColumnFromBasicsTable(const std::string& filePath, uint32_t* data) {
    std::ifstream file(filePath);
    if (!file.is_open()) {
        std::cerr << "Error opening file: " << filePath << std::endl;
        exit(1);
    }

    std::string line;
    int index = 0;
    bool isFirstRow = true;  // Flag to skip the first row
    while (std::getline(file, line)) {
        if (isFirstRow) {
            isFirstRow = false;
            continue;  // Skip the first row
        }

        std::istringstream iss(line);
        std::string value;
        for (size_t i = 0; i < 6; ++i) {
            std::getline(iss, value, '\t');
        }

        try {
            data[index] = std::stoi(value);
        } catch (const std::invalid_argument &e) {
            data[index] = 0;
        }
        index++;
    }

    file.close();
}

void readImdbTitleIdColumnFromPrincipalsTable(const std::string& filePath, uint32_t* data) {
    std::ifstream file(filePath);
    if (!file.is_open()) {
        std::cerr << "Error opening file: " << filePath << std::endl;
        exit(1);
    }

    std::string line;
    int index = 0;
    bool isFirstRow = true;  // Flag to skip the first row
    while (std::getline(file, line)) {
        if (isFirstRow) {
            isFirstRow = false;
            continue;  // Skip the first row
        }

        std::istringstream iss(line);
        std::string titleId, titleNumOnly;

        std::getline(iss, titleId, '\t');
        titleNumOnly = titleId.substr(2);
        data[index++] = std::stoi(titleNumOnly);
    }

    file.close();
}

void readImdbPersonIdColumnFromPrincipalsTable(const std::string& filePath, uint32_t* data) {
    std::ifstream file(filePath);
    if (!file.is_open()) {
        std::cerr << "Error opening file: " << filePath << std::endl;
        exit(1);
    }

    std::string line;
    int index = 0;
    bool isFirstRow = true;  // Flag to skip the first row
    while (std::getline(file, line)) {
        if (isFirstRow) {
            isFirstRow = false;
            continue;  // Skip the first row
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

void readImdbTitleIdColumnFromAkasTable(const std::string& filePath, int* data) {
    std::ifstream file(filePath);
    if (!file.is_open()) {
        std::cerr << "Error opening file: " << filePath << std::endl;
        exit(1);
    }

    std::string line;
    int index = 0;
    bool isFirstRow = true;  // Flag to skip the first row
    while (std::getline(file, line)) {
        if (isFirstRow) {
            isFirstRow = false;
            continue;  // Skip the first row
        }

        std::istringstream iss(line);
        std::string filmId, filmNumOnly;

        std::getline(iss, filmId, '\t');
        filmNumOnly = filmId.substr(2);
        data[index++] = std::stoi(filmNumOnly);
    }

    file.close();
}

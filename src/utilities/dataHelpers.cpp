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

void addLeadingZeros(std::string& input, size_t desiredLength) {
    if (input.length() >= desiredLength) {
        return;
    }

    size_t zerosToAdd = desiredLength - input.length();
    input.insert(0, zerosToAdd, '0');
}

void readOisstDataFromCsv(std::string& filePath, int n, int64_t *yearLatLong, int* monthDay, float *sst) {
    std::ifstream file(filePath);
    if (!file.is_open()) {
        std::cerr << "Error opening file: " << filePath << std::endl;
        exit(1);
    }

    std::cout << "Reading data from csv... ";

    // Skip the first line of data (header)
    std::string headerLine;
    std::getline(file, headerLine);

    std::string line;
    for (int i = 0; i < n && std::getline(file, line); ++i) {
        std::istringstream ss(line);
        std::string temp;

        std::getline(ss, temp, ','); // Read yearLatLong
        std::istringstream iss(temp);

        std::string yearStr, latitudeStr, longitudeStr;
        std::getline(iss, yearStr, ':');
        std::getline(iss, latitudeStr, ':');
        std::getline(iss, longitudeStr, ':');

/*        int64_t year = std::stoi(yearStr);
        float latitudeFloat = std::stof(latitudeStr);
        float longitudeFloat = std::stof(longitudeStr);

        int latitude = static_cast<int>(((latitudeFloat - 0.125) /0.25) + 360);
        int longitude = static_cast<int>((longitudeFloat - 0.125) /0.25);

        yearLatLong[i] = (year * 10000 * 10000) + (latitude * 10000) + longitude;*/

        std::hash<std::string> hashFunc;
        yearLatLong[i] = hashFunc(yearStr + latitudeStr + longitudeStr);

        std::getline(ss, temp, ','); // Read monthDay
        monthDay[i] = stoi(temp);

        std::getline(ss, temp, ','); // Read sst
        try {
            sst[i] = std::stof(temp);
        } catch (const std::invalid_argument& e) {
            sst[i] = std::numeric_limits<float>::quiet_NaN();
        }
    }

    std::cout << "Complete" << std::endl;
    file.close();
}

void writeOisstDataToCsv(std::string& filePath, int n, std::string *yearLatLong, float *sst) {
    std::ofstream file(filePath);
    std::cout << "Writing file to csv... ";

    file << "yearLatLong,sst\n";

    if (file.is_open()) {
        for (int i = 0; i < n; ++i) {
            file << yearLatLong[i] << "," << sst[i] << "\n";
        }
        file.close();
    } else {
        std::cerr << "Failed to open file: " << filePath << "\n";
    }

    std::cout << "Complete" << std::endl;
}

void readImdbStartYearColumn(const std::string& filePath, int* data) {
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
            data[index++] = std::stoi(value);
        } catch (const std::invalid_argument &e) {
            data[index++] = 9999;
        }
    }

    file.close();
}

void readImdbStartYearColumn(const std::string& filePath, uint32_t* data) {
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
            data[index++] = std::stoi(value);
        } catch (const std::invalid_argument &e) {
            data[index++] = 9999;
        }
    }

    file.close();
}

bool isNumber(const std::string& str) {
    for (char c : str) {
        if (!std::isdigit(c)) {
            return false;
        }
    }
    return true;
}

void readImdbParentTvSeriesAndSeasonColumn(const std::string& filePath, int64_t* data) {
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
        std::string dummy, seriesId, seriesNumberOnly, seasonNum;

        std::getline(iss, dummy, '\t');
        std::getline(iss, seriesId, '\t');
        std::getline(iss, seasonNum, '\t');

        seriesNumberOnly = seriesId.substr(2);
        addLeadingZeros(seriesNumberOnly, 10);

        if (!isNumber(seasonNum)) seasonNum = "1";
        addLeadingZeros(seasonNum, 4);

        data[index++] = std::stoll(seasonNum + seriesNumberOnly);
    }

    file.close();
}

void readImdbParentTvSeriesAndSeasonColumn(const std::string& filePath, uint64_t* data) {
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
        std::string dummy, seriesId, seriesNumberOnly, seasonNum;

        std::getline(iss, dummy, '\t');
        std::getline(iss, seriesId, '\t');
        std::getline(iss, seasonNum, '\t');

        seriesNumberOnly = seriesId.substr(2);
        addLeadingZeros(seriesNumberOnly, 10);

        if (!isNumber(seasonNum)) seasonNum = "1";
        addLeadingZeros(seasonNum, 4);

        data[index++] = std::stoll(seasonNum + seriesNumberOnly);
    }

    file.close();
}

void readImdbPrincipalsColumn(const std::string& filePath, int* data) {
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

void readImdbFilmsColumnFromPrincipals(const std::string& filePath, int* data) {
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
        std::string dummy1, dummy2, filmId, filmNumOnly;

        std::getline(iss, dummy1, '\t');
        std::getline(iss, dummy2, '\t');
        std::getline(iss, filmId, '\t');

        filmNumOnly = filmId.substr(2);
        data[index++] = std::stoi(filmNumOnly);
    }

    file.close();
}

void readImdbFilmColumn(const std::string& filePath, int64_t* data) {
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

void readImdbDirectorsColumn(const std::string& filePath, int* data) {
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
        std::string dummy, directorId, directorNumOnly;

        std::getline(iss, dummy, '\t');
        std::getline(iss, directorId, '\t');

        size_t commaPosition = directorId.find(',');
        if (commaPosition != std::string::npos) {
            directorId = directorId.substr(0, commaPosition);
        }
        directorNumOnly = directorId.substr(2);

        try {
            data[index] = std::stoi(directorNumOnly);
        } catch (const std::invalid_argument&) {
            data[index] = -1;
        }
        index++;
    }

    file.close();
}



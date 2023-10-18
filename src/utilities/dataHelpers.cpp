#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <cassert>
#include <stdexcept>
#include <cmath>

#include "dataHelpers.h"


int getLengthOfFile(const std::string& filePath) {
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
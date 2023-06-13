#include <utility>

#include "dataFiles.h"

const std::string outputFilePath = "/home/jack/CLionProjects/micro-adaptive-bulk-processing-library/data/output/";
const std::string selectCyclesFolder = "select_cycles_benchmark/";

const DataFile DataFiles::uniformIntDistribution25kValues{
    "/home/jack/CLionProjects/micro-adaptive-bulk-processing-library/data/input/uniformIntDistribution25kValues.csv",
    25*1000,
    "uniformIntDistribution25kValues"};

const DataFile DataFiles::uniformIntDistribution250mValues{
        "/home/jack/CLionProjects/micro-adaptive-bulk-processing-library/data/input/uniformIntDistribution250mValues.csv",
        250*1000*1000,
        "uniformIntDistribution250mValues"};


DataFile::DataFile(std::string _filePath, int _numElements, std::string _fileName)
        : filePath(std::move(_filePath)), numElements(_numElements), fileName(std::move(_fileName)) {}

const std::string& DataFile::getFilepath() const {
    return filePath;
}

int DataFile::getNumElements() const {
    return numElements;
}

const std::string& DataFile::getFileName() const {
    return fileName;
}

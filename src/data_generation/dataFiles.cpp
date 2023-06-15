#include <utility>

#include "dataFiles.h"
#include "dataGenerators.h"

const std::string inputFilePath = "/home/jack/CLionProjects/micro-adaptive-bulk-processing-library/data/input/";
const std::string outputFilePath = "/home/jack/CLionProjects/micro-adaptive-bulk-processing-library/data/output/";
const std::string selectCyclesFolder = "select_cycles_benchmark/";

const DataFile DataFiles::uniformIntDistribution25kValues{
    25 * 1000,
    "uniformIntDistribution25kValues",
    "Uniform distribution of 25k int values"};

const DataFile DataFiles::uniformIntDistribution250mValues{
    250 * 1000 * 1000,
    "uniformIntDistribution250mValues",
    "uniform distribution of 250m int values"};

const DataFile DataFiles::varyingIntDistribution25kValues{
    25 * 1000,
    "varyingIntDistribution25kValues",
    "25k int values where max value is 100 and min value varies linearly from 0 to 50 a number of times"};

const DataFile DataFiles::varyingIntDistribution250mValues{
    250 * 1000 * 1000,
    "varyingIntDistribution250mValues",
    "250m int values where max value is 100 and min value varies linearly from 0 to 50 a number of times"};

const DataFile DataFiles::step50IntDistribution25kValues{
    25 * 1000,
    "step50IntDistribution25kValues",
    "25k values where max value is 100 and min value is in blocks of either 0 or 50"};

const DataFile DataFiles::step50IntDistribution250mValues{
    250 * 1000 * 1000,
    "step50IntDistribution250mValues",
    "250m values where max value is 100 and min value is in blocks of either 0 or 50"};

DataFile::DataFile(int _numElements, std::string _fileName, std::string _longDescription)
        : numElements(_numElements), fileName(std::move(_fileName)), longDescription(std::move(_longDescription)) {}

void DataFile::loadDataIntoMemory(int *data) const {
    if (getFileName() == "uniformIntDistribution25kValues") {
        generateUniformDistributionInMemory(data, getNumElements());
    } else if (getFileName() == "uniformIntDistribution250mValues") {
        generateUniformDistributionInMemory(data, getNumElements());
    } else if (getFileName() == "varyingIntDistribution25kValues") {
        generateVaryingSelectivityInMemory(data, getNumElements(), 50, 10);
    } else if (getFileName() == "varyingIntDistribution250mValues") {
        generateVaryingSelectivityInMemory(data, getNumElements(), 50, 10);
    } else if (getFileName() == "step50IntDistribution25kValues") {
        generateStepSelectivityInMemory(data, getNumElements(), 50, 10);
    } else if (getFileName() == "step50IntDistribution250mValues") {
        generateStepSelectivityInMemory(data, getNumElements(), 50, 10);
    }
};

const std::string& DataFile::getLongDescription() const {
    return longDescription;
}

int DataFile::getNumElements() const {
    return numElements;
}

const std::string& DataFile::getFileName() const {
    return fileName;
}

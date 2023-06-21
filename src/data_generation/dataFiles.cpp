#include <utility>

#include "dataFiles.h"
#include "dataGenerators.h"
#include "../utils/dataHelpers.h"

const std::string selectCyclesFolder = "select_cycles_benchmark/";
const std::string dataFilesFolder = "dataFiles/";

const DataFile DataFiles::uniformIntDistribution25kValuesMax100{
    25 * 1000,
    "uniformIntDistribution25kValuesMax100",
    "Uniform distribution of 25k int values 1-100"};

const DataFile DataFiles::uniformIntDistribution250mValuesMax100{
    250 * 1000 * 1000,
    "uniformIntDistribution250mValuesMax100",
    "uniform distribution of 250m int values 1-100"};

const DataFile DataFiles::uniformIntDistribution250mValuesMax10000{
        250 * 1000 * 1000,
        "uniformIntDistribution250mValuesMax10000",
        "uniform distribution of 250m int values 1-10,000"};

const DataFile DataFiles::uniformIntDistribution250mValuesMax1000000{
        250 * 1000 * 1000,
        "uniformIntDistribution250mValuesMax1000000",
        "uniform distribution of 250m int values 1-1000,000"};

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

const DataFile DataFiles::worstCaseTunedStep50IntDistribution250mValues{
        250 * 1000 * 1000,
        "worstCaseTunedStep50IntDistribution250mValues",
        "250m values where max value is 100 and min value is in blocks of either 0 or 50 - size of blocks tuned to act as worst case"};

const DataFile DataFiles::bestCaseTunedUnequalStep50IntDistribution250mValues{
        250 * 1000 * 1000,
        "bestCaseTunedUnequalStep50IntDistribution250mValues",
        "250m values where max value is 100 and min value is in blocks of either 0 or 50. Blocks of 0 are 10 times larger than blocks of 0."};

const DataFile DataFiles::fullySortedIntDistribution250mValues{
        250 * 1000 * 1000,
        "fullySortedIntDistribution250mValues",
        ""};

const DataFile DataFiles::veryNearlyFullySortedIntDistribution250mValues{
        250 * 1000 * 1000,
        "veryNearlyFullySortedIntDistribution250mValues",
        ""};

const DataFile DataFiles::nearlyFullySortedIntDistribution250mValues{
        250 * 1000 * 1000,
        "nearlyFullySortedIntDistribution250mValues",
        ""};

const DataFile DataFiles::partiallySortedIntDistribution250mValues{
        250 * 1000 * 1000,
        "partiallySortedIntDistribution250mValues",
        ""};

const DataFile DataFiles::slightlySortedIntDistribution250mValues{
        250 * 1000 * 1000,
        "slightlySortedIntDistribution250mValues",
        ""};


DataFile::DataFile(int _numElements, std::string _fileName, std::string _longDescription)
        : numElements(_numElements), fileName(std::move(_fileName)), longDescription(std::move(_longDescription)) {}

void DataFile::loadDataIntoMemory(int *data) const {
    if (getFileName() == "uniformIntDistribution25kValuesMax100") {
        generateUniformDistributionInMemory(data, getNumElements(), 100);
    } else if (getFileName() == "uniformIntDistribution250mValuesMax100") {
        generateUniformDistributionInMemory(data, getNumElements(), 100);
    } else if (getFileName() == "uniformIntDistribution250mValuesMax10000") {
        generateUniformDistributionInMemory(data, getNumElements(), 10*1000);
    } else if (getFileName() == "uniformIntDistribution250mValuesMax1000000") {
        generateUniformDistributionInMemory(data, getNumElements(), 1000*1000);
    } else if (getFileName() == "varyingIntDistribution25kValues") {
        generateVaryingSelectivityInMemory(data, getNumElements(), 50, 10);
    } else if (getFileName() == "varyingIntDistribution250mValues") {
        generateVaryingSelectivityInMemory(data, getNumElements(), 50, 10);
    } else if (getFileName() == "step50IntDistribution25kValues") {
        generateStepSelectivityInMemory(data, getNumElements(), 50, 10);
    } else if (getFileName() == "step50IntDistribution250mValues") {
        generateStepSelectivityInMemory(data, getNumElements(), 50, 10);
    } else if (getFileName() == "worstCaseTunedStep50IntDistribution250mValues") {
        generateStepSelectivityInMemory(data, getNumElements(), 50, 5000);
    } else if (getFileName() == "bestCaseTunedUnequalStep50IntDistribution250mValues") {
        generateUnequalStepSelectivityInMemory(data, getNumElements(), 50, 10, 10);
    } else if (getFileName() == "fullySortedIntDistribution250mValues") {
        generatePartiallySortedInMemory(data, getNumElements(), 100, 0);
    } else if (getFileName() == "veryNearlyFullySortedIntDistribution250mValues") {
        generatePartiallySortedInMemory(data, getNumElements(), 100, 0.01);
    } else if (getFileName() == "nearlyFullySortedIntDistribution250mValues") {
        generatePartiallySortedInMemory(data, getNumElements(), 100, 1);
    } else if (getFileName() == "partiallySortedIntDistribution250mValues") {
        generatePartiallySortedInMemory(data, getNumElements(), 100, 5);
    } else if (getFileName() == "slightlySortedIntDistribution250mValues") {
        generatePartiallySortedInMemory(data, getNumElements(), 100, 25);
    }
}

const std::string& DataFile::getLongDescription() const {
    return longDescription;
}

int DataFile::getNumElements() const {
    return numElements;
}

const std::string& DataFile::getFileName() const {
    return fileName;
}

DataSweep DataSweeps::logSortedIntDistribution25kValuesRandomnessSweep {
        30,
        25*1000,
        "logSortedIntDistribution25kValuesRandomnessSweep",
        "Partially sorted distribution of 25k int values. Log input of percentage randomness"};

DataSweep DataSweeps::logSortedIntDistribution250mValuesRandomnessSweep {
        30,
        250*1000*1000,
        "logSortedIntDistribution250mValuesRandomnessSweep",
        "Partially sorted distribution of 250m int values. Log input of percentage randomness"};

DataSweep DataSweeps::varyingIntDistribution250mValuesSweep {
        30,
        250*1000*1000,
        "varyingIntDistribution250mValuesSweep",
        ""};

DataSweep DataSweeps::step50IntDistribution250mValuesSweep {
        30,
        250*1000*1000,
        "step50IntDistribution250mValuesSweep",
        ""};


DataSweep::DataSweep(int _totalRuns, int _numElements, std::string _sweepName, std::string _longDescription)
        : totalRuns(_totalRuns), numElements(_numElements), runsCompleted(0), sweepName(std::move(_sweepName)),
        longDescription(std::move(_longDescription)) {
    if (getSweepName() == "logSortedIntDistribution25kValuesRandomnessSweep" ||
        getSweepName() == "logSortedIntDistribution250mValuesRandomnessSweep") {
        generateLogDistribution(getTotalRuns(), 0.01, 100, inputs);
    } else if (getSweepName() == "varyingIntDistribution250mValuesSweep" ||
               getSweepName() == "step50IntDistribution250mValuesSweep") {
        generateLogDistribution(getTotalRuns(), 2, 10000, inputs);
    }
}

bool DataSweep::loadNextDataSetIntoMemory(int *data) {
    if (runsCompleted == totalRuns) {
        return false;
    }

    if (getSweepName() == "logSortedIntDistribution25kValuesRandomnessSweep") {
        generatePartiallySortedInMemory(data, getNumElements(), 10, inputs[runsCompleted++]);
        return true;
    } else if (getSweepName() == "logSortedIntDistribution250mValuesRandomnessSweep") {
        generatePartiallySortedInMemory(data, getNumElements(), 10, inputs[runsCompleted++]);
        return true;
    } else if (getSweepName() == "varyingIntDistribution250mValuesSweep") {
        generateVaryingSelectivityInMemory(data, getNumElements(), 50, static_cast<int>(inputs[runsCompleted++]));
        return true;
    } else if (getSweepName() == "step50IntDistribution250mValuesSweep") {
        generateStepSelectivityInMemory(data, getNumElements(), 50, static_cast<int>(inputs[runsCompleted++]));
        return true;
    }
    return false;
}

const std::string& DataSweep::getLongDescription() const {
    return longDescription;
}

int DataSweep::getTotalRuns() const {
    return totalRuns;
}

int DataSweep::getNumElements() const {
    return numElements;
}

const std::string& DataSweep::getSweepName() const {
    return sweepName;
}

float DataSweep::getRunInput() const {
    return inputs[runsCompleted];
}

void DataSweep::restartSweep() {
    runsCompleted = 0;
}


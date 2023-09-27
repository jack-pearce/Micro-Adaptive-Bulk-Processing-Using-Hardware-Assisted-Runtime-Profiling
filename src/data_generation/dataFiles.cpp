#include "dataFiles.h"
#include "../library/utilities/systemInformation.h"


FilePaths& FilePaths::getInstance() {
    static FilePaths instance;
    return instance;
}

FilePaths::FilePaths() {
    projectFilePath = MABPL::getCurrentWorkingDirectory();
}

std::string FilePaths::getSelectCyclesFolderPath() {
    return projectFilePath +selectCyclesFolder;
}

std::string FilePaths::getGroupByCyclesFolderPath() {
    return projectFilePath + groupByCyclesFolder;
}

std::string FilePaths::getPartitionCyclesFolderPath() {
    return projectFilePath + partitionCyclesFolder;
}

std::string FilePaths::getDataFilesFolderPath() {
    return projectFilePath + dataFilesFolder;
}

std::string FilePaths::getOisstInputFolderPath() {
    return projectFilePath + oisstInputFolder;
}

std::string FilePaths::getOisstOutputFolderPath() {
    return projectFilePath + oisstOnputFolder;
}

std::string FilePaths::getImdbInputFolderPath() {
    return projectFilePath + imdbInputFolder;
}

std::string FilePaths::getImdbOutputFolderPath() {
    return projectFilePath + imdbOutputFolder;
}

void generateEvenIntLogDistribution(int numPoints, double minValue, double maxValue, std::vector<float> &points) {
    for (auto i = 0; i < numPoints; ++i) {
        auto t = i / static_cast<double>(numPoints - 1); // Normalized parameter between 0 and 1
        auto value = std::pow(10.0, t * (std::log10(maxValue) - std::log10(minValue)) + std::log10(minValue));
        points.push_back(static_cast<float>(value));
    }

    int tmp;
    for (float &point : points) {
        tmp = static_cast<int>(point);
        if (tmp % 2 == 1) {
            tmp++;
        }
        point = tmp;
    }
}

void generateLogDistribution(int numPoints, double minValue, double maxValue, std::vector<float> &points) {
    for (auto i = 0; i < numPoints; ++i) {
        auto t = i / static_cast<double>(numPoints - 1); // Normalized parameter between 0 and 1
        auto value = std::pow(10.0, t * (std::log10(maxValue) - std::log10(minValue)) + std::log10(minValue));
        points.push_back(static_cast<float>(value));
    }
}

void generateLinearDistribution(int numPoints, double minValue, double maxValue, std::vector<float> &points) {
    auto step = (maxValue - minValue) / (numPoints - 1);
    for (auto i = 0; i < numPoints; i++) {
        auto value = minValue + i * step;
        points.push_back(static_cast<float>(value));
    }
}

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
        "25k int values where max value is 100 and min value varies linearly from 1 to 51 ten times"};

const DataFile DataFiles::varyingIntDistribution250mValues{
        250 * 1000 * 1000,
        "varyingIntDistribution250mValues",
        "250m int values where max value is 100 and min value varies linearly from 1 to 51 ten times"};

const DataFile DataFiles::upperStep50IntDistribution25kValues{
        25 * 1000,
        "upperStep50IntDistribution25kValues",
        "25k values where min value is 1 and max value is in ten blocks of either 51 or 100"};

const DataFile DataFiles::upperStep50IntDistribution250mValues{
        250 * 1000 * 1000,
        "upperStep50IntDistribution250mValues",
        "250m values where min value is 1 and max value is in ten blocks of either 51 or 100"};

const DataFile DataFiles::worstCaseIndexesTunedUpperStep50IntDistribution250mValues{
        250 * 1000 * 1000,
        "worstCaseIndexesTunedUpperStep50IntDistribution250mValues",
        "250m values where min value is 1 and max value is in 5000 blocks of either 50 or 100"};

const DataFile DataFiles::worstCaseValuesTunedLowerStep50IntDistribution250mValues{
        250 * 1000 * 1000,
        "worstCaseValuesTunedLowerStep50IntDistribution250mValues",
        "250m values where max value is 100 and min value is in 5000 blocks of either 1 or 51"};

const DataFile DataFiles::bestCaseIndexesTunedUnequalLowerStep50IntDistribution250mValues{
        250 * 1000 * 1000,
        "bestCaseIndexesTunedUnequalLowerStep50IntDistribution250mValues",
        "250m values where max value is 100 and min value is in blocks of either 1 or 51. "
        "Blocks of 51 are 10 times larger than blocks of 1."};

const DataFile DataFiles::bestCaseValuesTunedUnequalLowerStep50IntDistribution250mValues{
        250 * 1000 * 1000,
        "bestCaseValuesTunedUnequalLowerStep50IntDistribution250mValues",
        "250m values where max value is 100 and min value is in blocks of either 1 or 51. "
        "Blocks of 51 are 15 times larger than blocks of 1."};

const DataFile DataFiles::fullySortedIntDistribution250mValues{
        250 * 1000 * 1000,
        "fullySortedIntDistribution250mValues",
        "250m values between which are sorted. Each value is repeated 100 times before the next value."};

const DataFile DataFiles::veryNearlyFullySortedIntDistribution250mValues{
        250 * 1000 * 1000,
        "veryNearlyFullySortedIntDistribution250mValues",
        "250m values between which are sorted. Each value is repeated 100 times before the next value."
        "0.01% of values are randomly swapped"};

const DataFile DataFiles::nearlyFullySortedIntDistribution250mValues{
        250 * 1000 * 1000,
        "nearlyFullySortedIntDistribution250mValues",
        "250m values between which are sorted. Each value is repeated 100 times before the next value."
        "1% of values are randomly swapped"};

const DataFile DataFiles::partiallySortedIntDistribution250mValues{
        250 * 1000 * 1000,
        "partiallySortedIntDistribution250mValues",
        "250m values between which are sorted. Each value is repeated 100 times before the next value."
        "5% of values are randomly swapped"};

const DataFile DataFiles::slightlySortedIntDistribution250mValues{
        250 * 1000 * 1000,
        "slightlySortedIntDistribution250mValues",
        "250m values between which are sorted. Each value is repeated 100 times before the next value."
        "25% of values are randomly swapped"};

const DataFile DataFiles::uniformIntDistribution200mValuesCardinality400kMax200m{
        200 * 1000 * 1000,
        "uniformIntDistribution200mValuesCardinality400kMax200m",
        "200m values with 400k unique values. Max of 200m i.e. gaps in distribution."};

const DataFile DataFiles::uniformIntDistribution200mValuesCardinality4175320Max200mClustered1k{
        200 * 1000 * 1000,
        "uniformIntDistribution200mValuesCardinality4175320Max200mClustered1k",
        "200m values with ~400k unique values. Max of 200m i.e. gaps in distribution. 1k clustering"};

const DataFile DataFiles::uniformIntDistribution20mValuesCardinality400kMax20m{
        20 * 1000 * 1000,
        "uniformIntDistribution20mValuesCardinality400kMax20m",
        "20m values with ~400k unique values. Max of 20m i.e. gaps in distribution."};

const DataFile DataFiles::uniformIntDistribution20mValuesCardinality10mMax20mClustered1k{
        20 * 1000 * 1000,
        "uniformIntDistribution20mValuesCardinality10mMax20mClustered1k",
        "20m values with ~10m unique values. Max of 20m i.e. gaps in distribution. 1k clustering"};

const DataFile DataFiles::uniformIntDistribution20mValuesCardinality10mMax20m{
        20 * 1000 * 1000,
        "uniformIntDistribution20mValuesCardinality10mMax20m",
        "20m values with ~10m unique values. Max of 20m i.e. gaps in distribution"};

const DataFile DataFiles::uniformIntDistribution20mValuesCardinality200kMax20m{
        20 * 1000 * 1000,
        "uniformIntDistribution20mValuesCardinality200kMax20m",
        "20m values with ~200k unique values. Max of 20m i.e. gaps in distribution"};

const DataFile DataFiles::uniformIntDistribution20mValuesCardinality50kMax20m{
        20 * 1000 * 1000,
        "uniformIntDistribution20mValuesCardinality50kMax20m",
        "20m values with ~50k unique values. Max of 20m i.e. gaps in distribution"};

const DataFile DataFiles::uniformIntDistribution20mValuesTwo10mCardinalitySections_100_10m_Max20m{
        20 * 1000 * 1000,
        "uniformIntDistribution20mValuesTwo10mCardinalitySections_100_10m_Max20m",
        ""};

const DataFile DataFiles::uniformIntDistribution250mValuesMax250m{
        250 * 1000 * 1000,
        "uniformIntDistribution250mValuesMax250m",
        ""};

const DataFile DataFiles::fullySortedIntDistribution250mValuesMax250m{
        250 * 1000 * 1000,
        "fullySortedIntDistribution250mValuesMax250m",
        ""};

const DataFile DataFiles::uniqueRandom250mInt{
        250 * 1000 * 1000,
        "uniqueRandom250mInt",
        ""};

const DataFile DataFiles::uniformIntDistribution20mValuesMax20m{
        20 * 1000 * 1000,
        "uniformIntDistribution20mValuesMax20m",
        ""};

const DataFile DataFiles::fullySortedIntDistribution20mValuesMax20m{
        20 * 1000 * 1000,
        "fullySortedIntDistribution20mValuesMax20m",
        ""};

const DataFile DataFiles::slightlyClusteredDistribution250mValuesCardinality10mMax250m{
        250 * 1000 * 1000,
        "slightlyClusteredDistribution250mValuesCardinality10mMax250m",
        ""};

const DataFile DataFiles::Clustered1mDistribution250mValuesCardinality10mMax250m{
        250 * 1000 * 1000,
        "Clustered1mDistribution250mValuesCardinality10mMax250m",
        ""};



DataFile::DataFile(int _numElements, std::string _fileName, std::string _longDescription)
        : numElements(_numElements), fileName(std::move(_fileName)), longDescription(std::move(_longDescription)) {}



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
        "Partially sorted distribution of 25k int values. "
        "Log input of percentage randomness from 0.01% - 100%"};

DataSweep DataSweeps::logSortedIntDistribution250mValuesRandomnessSweep {
        30,
        250*1000*1000,
        "logSortedIntDistribution250mValuesRandomnessSweep",
        "Partially sorted distribution of 250m int values. "
        "Log input of percentage randomness from 0.01% - 100%"};

DataSweep DataSweeps::varyingIntDistribution250mValuesSweep {
        30,
        250*1000*1000,
        "varyingIntDistribution250mValuesSweep",
        "250m int values where max value is 100 and min value varies linearly from 1 to 51. "
        "Linear input of number of discrete sections from 2 to 10,000"};

DataSweep DataSweeps::lowerStep50IntDistribution250mValuesSweep {
        30,
        250*1000*1000,
        "lowerStep50IntDistribution250mValuesSweep",
        "250m int values where max value is 100 and min value is either 1 or 51"
        "Log input of number of discrete sections from 2 to 10,000"};

DataSweep DataSweeps::lowerStep50IntDistribution250mValuesSweepSectionLength_1 {
        30,
        250*1000*1000,
        "lowerStep50IntDistribution250mValuesSweepSectionLength_1",
        "250m int values where max value is 100 and min value is either 1 or 51"
        "Log input of discrete section length"};

DataSweep DataSweeps::lowerStep50IntDistribution250mValuesSweepSectionLength_2 {
        30,
        250*1000*1000,
        "lowerStep50IntDistribution250mValuesSweepSectionLength_2",
        "250m int values where max value is 100 and min value is either 1 or 51"
        "Log input of discrete section length"};

DataSweep DataSweeps::lowerStep50IntDistribution250mValuesSweepSectionLength_3 {
        60,
        250*1000*1000,
        "lowerStep50IntDistribution250mValuesSweepSectionLength_3",
        "250m int values where max value is 100 and min value is either 1 or 51"
        "Log input of discrete section length"};

DataSweep DataSweeps::lowerStep50IntDistribution250mValuesPercentageStepSweep {
        101,
        250*1000*1000,
        "lowerStep50IntDistribution250mValuesPercentageStepSweep",
        "250m int values where max value is 100 and min value is either 1 or 51"
        "Linear input of percentage of input which is 51"};

DataSweep DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepVariableMax {
        30,
        200*1000*1000,
        "logUniformIntDistribution200mValuesCardinalitySweepVariableMax",
        "Log distribution of cardinality for 200m ints from 1 to 100m. The max value is the cardinality"
        "value, so the distribution is dense i.e. there are no gaps."};

DataSweep DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMax {
        30,
        200*1000*1000,
        "logUniformIntDistribution200mValuesCardinalitySweepFixedMax",
        "Log distribution of cardinality for 200m ints from 1 to 100m. The max value is fixed at 200m, "
        "so the distribution is sparse i.e. there are gaps."};

DataSweep DataSweeps::logUniformIntDistribution200mValuesCardinalityUpTo10mSweepFixedMax {
        30,
        200*1000*1000,
        "logUniformIntDistribution200mValuesCardinalityUpTo10mSweepFixedMax",
        "Log distribution of cardinality for 200m ints from 1 to 10m. The max value is fixed at 200m, "
        "so the distribution is sparse i.e. there are gaps."};

DataSweep DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered1 {
        30,
        200*1000*1000,
        "logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered1",
        "Log distribution of cardinality for 200m ints from 1 to 100m. The max value is fixed at 200m, "
        "so the distribution is sparse i.e. there are gaps. The values are clustered in groups of 1 and"
        "increase smoothly i.e. only one value of the 1000 changes when moving from one cluster"
        "to the next cluster"};

DataSweep DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered1k {
        30,
        200*1000*1000,
        "logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered1k",
        "Log distribution of cardinality for 200m ints from 1 to 100m. The max value is fixed at 200m, "
        "so the distribution is sparse i.e. there are gaps. The values are clustered in groups of 1000 and"
        "increase smoothly i.e. only one value of the 1000 changes when moving from one cluster"
        "to the next cluster"};

DataSweep DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered100k {
        30,
        200*1000*1000,
        "logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered100k",
        "Log distribution of cardinality for 200m ints from 1 to 100m. The max value is fixed at 200m, "
        "so the distribution is sparse i.e. there are gaps. The values are clustered in groups of 100,000 and"
        "increase smoothly i.e. only one value of the 1000 changes when moving from one cluster"
        "to the next cluster"};

DataSweep DataSweeps::linearUniformIntDistribution200mValuesCardinalitySweepFixedMaxCrossOverPoint {
        22,
        200*1000*1000,
        "linearUniformIntDistribution200mValuesCardinalitySweepFixedMaxCrossOverPoint",
        "Linear distribution of cardinality for 200m ints in the groupBy cross over range"};

DataSweep DataSweeps::logUniformIntDistribution400mValuesCardinalitySweepFixedMax {
        30,
        400*1000*1000,
        "logUniformIntDistribution400mValuesCardinalitySweepFixedMax",
        "Log distribution of cardinality for 400m ints from 1 to 100m. The max value is fixed at 200m, "
        "so the distribution is sparse i.e. there are gaps."};

DataSweep DataSweeps::logUniformIntDistribution20mValuesCardinalitySweepVariableMax {
        30,
        20*1000*1000,
        "logUniformIntDistribution20mValuesCardinalitySweepVariableMax",
        "Log distribution of cardinality for 20m ints from 1 to 100m. The max value is the cardinality"
        "value, so the distribution is dense i.e. there are no gaps."};

DataSweep DataSweeps::logUniformIntDistribution20mValuesCardinalitySweepFixedMax {
        30,
        20*1000*1000,
        "logUniformIntDistribution20mValuesCardinalitySweepFixedMax",
        "Log distribution of cardinality for 20m ints from 1 to 100m. The max value is fixed at 200m, "
        "so the distribution is sparse i.e. there are gaps."};

DataSweep DataSweeps::logUniformIntDistribution20mValuesCardinalitySweepFixedMaxClustered10 {
        30,
        20*1000*1000,
        "logUniformIntDistribution20mValuesCardinalitySweepFixedMaxClustered10",
        "Log distribution of cardinality for 20m ints from 1 to 100m. The max value is fixed at 200m, "
        "so the distribution is sparse i.e. there are gaps. The values are clustered in groups of 10 and"
        "increase smoothly i.e. only one value of the 1000 changes when moving from one cluster"
        "to the next cluster"};

DataSweep DataSweeps::logUniformIntDistribution20mValuesCardinalitySweepFixedMaxClustered1k {
        30,
        20*1000*1000,
        "logUniformIntDistribution20mValuesCardinalitySweepFixedMaxClustered1k",
        "Log distribution of cardinality for 20m ints from 1 to 100m. The max value is fixed at 200m, "
        "so the distribution is sparse i.e. there are gaps. The values are clustered in groups of 1000 and"
        "increase smoothly i.e. only one value of the 1000 changes when moving from one cluster"
        "to the next cluster"};

DataSweep DataSweeps::logUniformIntDistribution20mValuesCardinalitySweepFixedMaxClustered100k {
        30,
        20*1000*1000,
        "logUniformIntDistribution20mValuesCardinalitySweepFixedMaxClustered100k",
        "Log distribution of cardinality for 20m ints from 1 to 100m. The max value is fixed at 200m, "
        "so the distribution is sparse i.e. there are gaps. The values are clustered in groups of 100,000 and"
        "increase smoothly i.e. only one value of the 1000 changes when moving from one cluster"
        "to the next cluster"};

DataSweep DataSweeps::logUniformIntDistribution40mValuesCardinalitySweepFixedMax {
        30,
        40*1000*1000,
        "logUniformIntDistribution40mValuesCardinalitySweepFixedMax",
        "Log distribution of cardinality for 40m ints from 1 to 100m. The max value is fixed at 200m, "
        "so the distribution is sparse i.e. there are gaps."};

DataSweep DataSweeps::linearUniformIntDistribution20mValuesCardinalitySections_100_10m_Max20m {
        30,
        20*1000*1000,
        "linearUniformIntDistribution20mValuesCardinalitySections_100_10m_Max20m",
        ""};

DataSweep DataSweeps::linearUniformIntDistribution200mValuesCardinalitySections_100_10m_Max20m {
        10,
        200*1000*1000,
        "linearUniformIntDistribution200mValuesCardinalitySections_100_10m_Max20m",
        ""};

DataSweep DataSweeps::linearUniformIntDistribution200mValuesCardinalitySections_100_100m_Max200m {
        10,
        200*1000*1000,
        "linearUniformIntDistribution200mValuesCardinalitySections_100_100m_Max200m",
        ""};

DataSweep DataSweeps::linearUniformIntDistribution20mValuesCardinalitySections_100_3m_Max20m {
        30,
        20*1000*1000,
        "linearUniformIntDistribution20mValuesCardinalitySections_100_3m_Max20m",
        ""};

DataSweep DataSweeps::linearUniformIntDistribution20mValuesCardinalitySections_3m_100_Max20m {
        30,
        20*1000*1000,
        "linearUniformIntDistribution20mValuesCardinalitySections_3m_100_Max20m",
        ""};

DataSweep DataSweeps::linearUniformIntDistribution20mValuesMultipleCardinalitySections_100_3m_Max20m {
        30,
        20*1000*1000,
        "linearUniformIntDistribution20mValuesMultipleCardinalitySections_100_3m_Max20m",
        ""};

DataSweep DataSweeps::linearUniformIntDistribution20mValuesMultipleCardinalitySections_3m_100_Max20m {
        30,
        20*1000*1000,
        "linearUniformIntDistribution20mValuesMultipleCardinalitySections_3m_100_Max20m",
        ""};

DataSweep DataSweeps::linearUniformIntDistribution200mValuesMultipleCardinalitySections_100_10m_Max100m {
        30,
        200*1000*1000,
        "linearUniformIntDistribution200mValuesMultipleCardinalitySections_100_10m_Max100m",
        ""};

DataSweep DataSweeps::linearUniformIntDistribution200mValuesMultipleCardinalitySections_10m_100_Max100m {
        30,
        200*1000*1000,
        "linearUniformIntDistribution200mValuesMultipleCardinalitySections_10m_100_Max100m",
        ""};

DataSweep DataSweeps::logUniformIntDistribution20mValuesClusteredSweepFixedCardinality1m {
        30,
        20*1000*1000,
        "logUniformIntDistribution20mValuesClusteredSweepFixedCardinality1m",
        ""};

DataSweep DataSweeps::logUniformIntDistribution200mValuesClusteredSweepFixedCardinality10mMax200m {
        30,
        200*1000*1000,
        "logUniformIntDistribution200mValuesClusteredSweepFixedCardinality10mMax200m",
        ""};

DataSweep DataSweeps::linearUniqueIntDistribution250mValuesSortednessSweep {
        26,
        250*1000*1000,
        "linearUniqueIntDistribution250mValuesSortednessSweep",
        ""};

DataSweep DataSweeps::logUniformIntDistribution250mValuesClusteredSweepFixedCardinality10mMax250m {
        30,
        250*1000*1000,
        "logUniformIntDistribution250mValuesClusteredSweepFixedCardinality10mMax250m",
        ""};

DataSweep DataSweeps::linearUniformIntDistribution250mValuesClusteredSweepFixedCardinality10mMax250m {
        30,
        250*1000*1000,
        "linearUniformIntDistribution250mValuesClusteredSweepFixedCardinality10mMax250m",
        ""};

DataSweep DataSweeps::logUniformIntDistribution200mValuesMaxValueSweep {
        30,
        200*1000*1000,
        "logUniformIntDistribution200mValuesMaxValueSweep",
        ""};


DataSweep::DataSweep(int _totalRuns, int _numElements, std::string _sweepName, std::string _longDescription)
        : totalRuns(_totalRuns), numElements(_numElements), runsCompleted(0), sweepName(std::move(_sweepName)),
          longDescription(std::move(_longDescription)) {
    if (getSweepName() == "logSortedIntDistribution25kValuesRandomnessSweep" ||
        getSweepName() == "logSortedIntDistribution250mValuesRandomnessSweep") {
        generateLogDistribution(getTotalRuns(), 0.01, 100, inputs);
    } else if (getSweepName() == "varyingIntDistribution250mValuesSweep" ||
               getSweepName() == "lowerStep50IntDistribution250mValuesSweep") {
        generateEvenIntLogDistribution(getTotalRuns(), 2, 10000, inputs);
    }  else if (getSweepName() == "lowerStep50IntDistribution250mValuesSweepSectionLength_1") {
        generateEvenIntLogDistribution(getTotalRuns(), 30000, 10000000, inputs);
    } else if (getSweepName() == "lowerStep50IntDistribution250mValuesSweepSectionLength_2") {
        generateLinearDistribution(getTotalRuns(), 1000, 30000, inputs);
    } else if (getSweepName() == "lowerStep50IntDistribution250mValuesSweepSectionLength_3") {
        generateLinearDistribution(getTotalRuns() - 1, 30000, 115000, inputs);
        inputs.push_back(50000.0);
    } else if (getSweepName() == "lowerStep50IntDistribution250mValuesPercentageStepSweep") {
        generateLinearDistribution(getTotalRuns(), 0, 1, inputs);
    } else if (getSweepName() == "logUniformIntDistribution200mValuesCardinalitySweepVariableMax" ||
               getSweepName() == "logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered1"  ||
               getSweepName() == "logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered1k"  ||
               getSweepName() == "logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered100k") {
        generateLogDistribution(getTotalRuns(), 1, getNumElements(), inputs);
    } else if (getSweepName() == "logUniformIntDistribution20mValuesCardinalitySweepFixedMaxClustered10"  ||
               getSweepName() == "logUniformIntDistribution20mValuesCardinalitySweepFixedMaxClustered1k"  ||
               getSweepName() == "logUniformIntDistribution20mValuesCardinalitySweepFixedMaxClustered100k") {
        generateLogDistribution(getTotalRuns(), 1, getNumElements(), inputs);
    } else if (getSweepName() == "logUniformIntDistribution20mValuesCardinalitySweepVariableMax" ||
               getSweepName() == "logUniformIntDistribution20mValuesCardinalitySweepFixedMax") {
        generateLogDistribution(getTotalRuns(), 1, getNumElements() / 2.0, inputs);
    } else if (getSweepName() == "logUniformIntDistribution40mValuesCardinalitySweepFixedMax" ||
               getSweepName() == "logUniformIntDistribution200mValuesCardinalitySweepFixedMax") {
        generateLogDistribution(getTotalRuns(), 1, getNumElements(), inputs);
    } else if (getSweepName() == "logUniformIntDistribution200mValuesCardinalityUpTo10mSweepFixedMax") {
        generateLogDistribution(getTotalRuns(), 1, 10*1000*1000, inputs);
    } else if (getSweepName() == "linearUniformIntDistribution200mValuesCardinalitySweepFixedMaxCrossOverPoint") {
        generateLinearDistribution(getTotalRuns(), 150000, 1200000, inputs);
    } else if (getSweepName() == "logUniformIntDistribution400mValuesCardinalitySweepFixedMax") {
        generateLogDistribution(getTotalRuns(), 1, 100000000, inputs);
    } else if (getSweepName() == "logUniformIntDistribution40mValuesCardinalitySweepFixedMax") {
        generateLogDistribution(getTotalRuns(), 1, 10000000, inputs);
    } else if (getSweepName() == "linearUniformIntDistribution20mValuesCardinalitySections_100_10m_Max20m" ||
               getSweepName() == "linearUniformIntDistribution200mValuesCardinalitySections_100_10m_Max20m" ||
               getSweepName() == "linearUniformIntDistribution200mValuesCardinalitySections_100_100m_Max200m" ||
               getSweepName() == "linearUniformIntDistribution20mValuesCardinalitySections_100_3m_Max20m"||
               getSweepName() == "linearUniformIntDistribution20mValuesCardinalitySections_3m_100_Max20m" ||
               getSweepName() == "linearUniformIntDistribution20mValuesMultipleCardinalitySections_100_3m_Max20m" ||
               getSweepName() == "linearUniformIntDistribution20mValuesMultipleCardinalitySections_3m_100_Max20m" ||
               getSweepName() == "linearUniformIntDistribution200mValuesMultipleCardinalitySections_100_10m_Max100m" ||
               getSweepName() == "linearUniformIntDistribution200mValuesMultipleCardinalitySections_10m_100_Max100m") {
        generateLinearDistribution(getTotalRuns(), 0, 1, inputs);
    } else if (getSweepName() == "logUniformIntDistribution20mValuesClusteredSweepFixedCardinality1m") {
        generateLogDistribution(getTotalRuns(), 1, 1000000, inputs);
    } else if (getSweepName() == "logUniformIntDistribution200mValuesClusteredSweepFixedCardinality10mMax200m") {
        generateLogDistribution(getTotalRuns(), 1, getNumElements(), inputs);
    } else if (getSweepName() == "linearUniqueIntDistribution250mValuesSortednessSweep") {
        generateLinearDistribution(getTotalRuns(), 0, 100, inputs);
    } else if (getSweepName() == "logUniformIntDistribution250mValuesClusteredSweepFixedCardinality10mMax250m") {
        generateLogDistribution(getTotalRuns(), 1, 10*1000*1000, inputs);
    } else if (getSweepName() == "linearUniformIntDistribution250mValuesClusteredSweepFixedCardinality10mMax250m") {
        generateLinearDistribution(getTotalRuns(), 1, 10*1000*1000, inputs);
    } else if (getSweepName() == "logUniformIntDistribution200mValuesMaxValueSweep") {
        generateLogDistribution(getTotalRuns(), 1, getNumElements(), inputs);
    }
}


int DataSweep::getCardinality() const {
    if (getSweepName() == "logUniformIntDistribution200mValuesCardinalitySweepVariableMax" ||
        getSweepName() == "logUniformIntDistribution200mValuesCardinalitySweepFixedMax" ||
        getSweepName() == "logUniformIntDistribution200mValuesCardinalityUpTo10mSweepFixedMax" ||
        getSweepName() == "logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered1" ||
        getSweepName() == "logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered1k" ||
        getSweepName() == "logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered100k" ||
        getSweepName() == "linearUniformIntDistribution200mValuesCardinalitySweepFixedMaxCrossOverPoint" ||
        getSweepName() == "logUniformIntDistribution400mValuesCardinalitySweepFixedMax" ||
        getSweepName() == "logUniformIntDistribution20mValuesCardinalitySweepVariableMax" ||
        getSweepName() == "logUniformIntDistribution20mValuesCardinalitySweepFixedMax" ||
        getSweepName() == "logUniformIntDistribution20mValuesCardinalitySweepFixedMaxClustered10" ||
        getSweepName() == "logUniformIntDistribution20mValuesCardinalitySweepFixedMaxClustered1k" ||
        getSweepName() == "logUniformIntDistribution20mValuesCardinalitySweepFixedMaxClustered100k" ||
        getSweepName() == "logUniformIntDistribution40mValuesCardinalitySweepFixedMax" ||
        getSweepName() == "logUniformIntDistribution20mValuesCardinalitySweepFixedMax") {
        return static_cast<int>(inputs[runsCompleted]);
    } else if (getSweepName() == "linearUniformIntDistribution20mValuesCardinalitySections_100_10m_Max20m" ||
               getSweepName() == "linearUniformIntDistribution200mValuesCardinalitySections_100_10m_Max20m") {
        return 10*1000*1000;
    } else if (getSweepName() == "linearUniformIntDistribution200mValuesCardinalitySections_100_100m_Max200m") {
        return 100*1000*1000;
    } else if (getSweepName() == "linearUniformIntDistribution20mValuesCardinalitySections_100_3m_Max20m" ||
               getSweepName() == "linearUniformIntDistribution20mValuesCardinalitySections_3m_100_Max20m") {
        return 3*1000*1000;
    } else if (getSweepName() == "linearUniformIntDistribution200mValuesMultipleCardinalitySections_100_10m_Max100m"  ||
                getSweepName() == "linearUniformIntDistribution200mValuesMultipleCardinalitySections_10m_100_Max100m") {
        return 10*1000*1000;
    } else if (getSweepName() == "logUniformIntDistribution20mValuesClusteredSweepFixedCardinality1m") {
        return 1*1000*1000;
    } else if (getSweepName() == "logUniformIntDistribution200mValuesClusteredSweepFixedCardinality10mMax200m") {
        return 10*1000*1000;
    } else {
        return -1;
    }
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

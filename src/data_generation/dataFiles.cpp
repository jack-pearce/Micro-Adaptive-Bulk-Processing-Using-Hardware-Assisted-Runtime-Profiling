#include <utility>

#include "dataFiles.h"
#include "dataGenerators.h"
#include "../utilities/dataHelpers.h"


FilePaths& FilePaths::getInstance() {
    static FilePaths instance;
    return instance;
}

FilePaths::FilePaths() {
    projectFilePath = std::filesystem::current_path();
}

std::string FilePaths::getSelectCyclesFolderPath() {
    return (projectFilePath / std::filesystem::path(selectCyclesFolder)).string();
}

std::string FilePaths::getGroupByCyclesFolderPath() {
    return (projectFilePath / std::filesystem::path(groupByCyclesFolder)).string();
}

std::string FilePaths::getDataFilesFolderPath() {
    return (projectFilePath / std::filesystem::path(dataFilesFolder)).string();
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
        generateVaryingSelectivityInMemory(data, getNumElements(), 51, 10);
    } else if (getFileName() == "varyingIntDistribution250mValues") {
        generateVaryingSelectivityInMemory(data, getNumElements(), 51, 10);
    } else if (getFileName() == "upperStep50IntDistribution25kValues") {
        generateUpperStepSelectivityInMemory(data, getNumElements(), 51, 10);
    } else if (getFileName() == "upperStep50IntDistribution250mValues") {
        generateUpperStepSelectivityInMemory(data, getNumElements(), 51, 10);
    } else if (getFileName() == "worstCaseIndexesTunedUpperStep50IntDistribution250mValues") {
        generateUpperStepSelectivityInMemory(data, getNumElements(), 50, 5000);
    } else if (getFileName() == "worstCaseValuesTunedLowerStep50IntDistribution250mValues") {
        generateLowerStepSelectivityInMemory(data, getNumElements(), 51, 5000);
    } else if (getFileName() == "bestCaseIndexesTunedUnequalLowerStep50IntDistribution250mValues") {
        generateUnequalLowerStepSelectivityInMemory(data, getNumElements(), 51, 10, 0.91);
    } else if (getFileName() == "bestCaseValuesTunedUnequalLowerStep50IntDistribution250mValues") {
        generateUnequalLowerStepSelectivityInMemory(data, getNumElements(), 51, 10, 0.94);
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
    } else if (getFileName() == "uniformIntDistribution200mValuesCardinality400kMax200m") {
        generateUniformDistributionInMemoryWithSetCardinality(data, getNumElements(), getNumElements(), 400000);
    } else if (getFileName() == "uniformIntDistribution200mValuesCardinality4175320Max200mClustered1k") {
        generateUniformDistributionInMemoryWithSetCardinalityClustered(data, getNumElements(), getNumElements(), 4175320, 1000);
    } else if (getFileName() == "uniformIntDistribution20mValuesCardinality400kMax20m") {
        generateUniformDistributionInMemoryWithSetCardinality(data, getNumElements(), 20*1000*1000, 400*1000);
    } else if (getFileName() == "uniformIntDistribution20mValuesCardinality10mMax20mClustered1k") {
        generateUniformDistributionInMemoryWithSetCardinalityClustered(data, getNumElements(), 20*1000*1000, 10*1000*1000, 1*1000);
    } else if (getFileName() == "uniformIntDistribution20mValuesCardinality10mMax20m") {
        generateUniformDistributionInMemoryWithSetCardinality(data, getNumElements(), 20*1000*1000, 10*1000*1000);
    } else if (getFileName() == "uniformIntDistribution20mValuesCardinality200kMax20m") {
        generateUniformDistributionInMemoryWithSetCardinality(data, getNumElements(), 20*1000*1000, 200*1000);
    } else if (getFileName() == "uniformIntDistribution20mValuesCardinality50kMax20m") {
        generateUniformDistributionInMemoryWithSetCardinality(data, getNumElements(), 20*1000*1000, 50*1000);
    } else if (getFileName() == "uniformIntDistribution20mValuesTwo10mCardinalitySections_100_10m_Max20m") {
        generateUniformDistributionInMemoryWithTwoCardinalitySections(data, getNumElements(), 20*1000*1000, 100, 10*1000*1000, 0.5);
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
        50,
        250*1000*1000,
        "lowerStep50IntDistribution250mValuesSweep",
        "250m int values where max value is 100 and min value is either 1 or 51"
        "Linear input of number of discrete sections from 2 to 10,000"};

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

DataSweep DataSweeps::logUniformInt64Distribution200mValuesCardinalitySweepFixedMax {
        30,
        200*1000*1000,
        "logUniformInt64Distribution200mValuesCardinalitySweepFixedMax",
        "Log distribution of cardinality for 200m ints from 1 to 100m. The max value is fixed at 200m, "
        "so the distribution is sparse i.e. there are gaps. Output is 64bit ints rather than 32bit ints."};

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

DataSweep DataSweeps::logUniformInt64Distribution20mValuesCardinalitySweepFixedMax {
        30,
        20*1000*1000,
        "logUniformInt64Distribution20mValuesCardinalitySweepFixedMax",
        "Log distribution of cardinality for 20m ints from 1 to 100m. The max value is fixed at 200m, "
        "so the distribution is sparse i.e. there are gaps. Output is 64bit ints rather than 32bit ints."};

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



DataSweep::DataSweep(int _totalRuns, int _numElements, std::string _sweepName, std::string _longDescription)
        : totalRuns(_totalRuns), numElements(_numElements), runsCompleted(0), sweepName(std::move(_sweepName)),
        longDescription(std::move(_longDescription)) {
    if (getSweepName() == "logSortedIntDistribution25kValuesRandomnessSweep" ||
        getSweepName() == "logSortedIntDistribution250mValuesRandomnessSweep") {
        generateLogDistribution(getTotalRuns(), 0.01, 100, inputs);
    } else if (getSweepName() == "varyingIntDistribution250mValuesSweep" ||
               getSweepName() == "lowerStep50IntDistribution250mValuesSweep") {
        generateEvenIntLogDistribution(getTotalRuns(), 2, 10000, inputs);
    } else if (getSweepName() == "lowerStep50IntDistribution250mValuesPercentageStepSweep") {
        generateLinearDistribution(getTotalRuns(), 0, 1, inputs);
    } else if (getSweepName() == "logUniformIntDistribution200mValuesCardinalitySweepVariableMax" ||
            getSweepName() == "logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered1"  ||
            getSweepName() == "logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered1k"  ||
            getSweepName() == "logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered100k" ||
            getSweepName() == "logUniformInt64Distribution200mValuesCardinalitySweepFixedMax") {
        generateLogDistribution(getTotalRuns(), 1, getNumElements() / 2.0, inputs);
    } else if (getSweepName() == "logUniformIntDistribution20mValuesCardinalitySweepFixedMaxClustered10"  ||
               getSweepName() == "logUniformIntDistribution20mValuesCardinalitySweepFixedMaxClustered1k"  ||
               getSweepName() == "logUniformIntDistribution20mValuesCardinalitySweepFixedMaxClustered100k") {
        generateLogDistribution(getTotalRuns(), 1, getNumElements() / 2.0, inputs);
    } else if (getSweepName() == "logUniformIntDistribution20mValuesCardinalitySweepVariableMax" ||
               getSweepName() == "logUniformIntDistribution20mValuesCardinalitySweepFixedMax" ||
               getSweepName() == "logUniformInt64Distribution20mValuesCardinalitySweepFixedMax") {
        generateLogDistribution(getTotalRuns(), 1, getNumElements(), inputs);
    } else if (getSweepName() == "logUniformIntDistribution40mValuesCardinalitySweepFixedMax" ||
               getSweepName() == "logUniformIntDistribution200mValuesCardinalitySweepFixedMax") {
        generateLogDistribution(getTotalRuns(), 1, getNumElements() / 2.0, inputs);
    }else if (getSweepName() == "linearUniformIntDistribution200mValuesCardinalitySweepFixedMaxCrossOverPoint") {
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
        generateVaryingSelectivityInMemory(data, getNumElements(), 51, static_cast<int>(inputs[runsCompleted++]));
        return true;
    } else if (getSweepName() == "lowerStep50IntDistribution250mValuesSweep") {
        generateLowerStepSelectivityInMemory(data, getNumElements(), 51, static_cast<int>(inputs[runsCompleted++]));
        return true;
    } else if (getSweepName() == "lowerStep50IntDistribution250mValuesPercentageStepSweep") {
        generateUnequalLowerStepSelectivityInMemory(data, getNumElements(), 51, 10, inputs[runsCompleted++]);
        return true;
    } else if (getSweepName() == "logUniformIntDistribution200mValuesCardinalitySweepVariableMax") {
        generateUniformDistributionInMemory(data, getNumElements(), static_cast<int>(inputs[runsCompleted++]));
        return true;
    } else if (getSweepName() == "logUniformIntDistribution200mValuesCardinalitySweepFixedMax") {
        generateUniformDistributionInMemoryWithSetCardinality(data, getNumElements(), 200*1000*1000, static_cast<int>(inputs[runsCompleted++]));
        return true;
    } else if (getSweepName() == "logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered1") {
        generateUniformDistributionInMemoryWithSetCardinalityClustered(data, getNumElements(), 200*1000*1000, static_cast<int>(inputs[runsCompleted++]), 1);
        return true;
    } else if (getSweepName() == "logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered1k") {
        generateUniformDistributionInMemoryWithSetCardinalityClustered(data, getNumElements(), 200*1000*1000, static_cast<int>(inputs[runsCompleted++]), 1000);
        return true;
    } else if (getSweepName() == "logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered100k") {
        generateUniformDistributionInMemoryWithSetCardinalityClustered(data, getNumElements(), 200*1000*1000, static_cast<int>(inputs[runsCompleted++]), 100000);
        return true;
    } else if (getSweepName() == "linearUniformIntDistribution200mValuesCardinalitySweepFixedMaxCrossOverPoint") {
        generateUniformDistributionInMemoryWithSetCardinality(data, getNumElements(), 200*1000*1000, static_cast<int>(inputs[runsCompleted++]));
        return true;
    } else if (getSweepName() == "logUniformIntDistribution400mValuesCardinalitySweepFixedMax") {
        generateUniformDistributionInMemoryWithSetCardinality(data, getNumElements(), 200*1000*1000, static_cast<int>(inputs[runsCompleted++]));
        return true;
    } else if (getSweepName() == "logUniformIntDistribution20mValuesCardinalitySweepVariableMax") {
        generateUniformDistributionInMemory(data, getNumElements(), static_cast<int>(inputs[runsCompleted++]));
        return true;
    } else if (getSweepName() == "logUniformIntDistribution20mValuesCardinalitySweepFixedMax") {
        generateUniformDistributionInMemoryWithSetCardinality(data, getNumElements(), 20*1000*1000, static_cast<int>(inputs[runsCompleted++]));
        return true;
    } else if (getSweepName() == "logUniformIntDistribution20mValuesCardinalitySweepFixedMaxClustered10") {
        generateUniformDistributionInMemoryWithSetCardinalityClustered(data, getNumElements(), 20*1000*1000, static_cast<int>(inputs[runsCompleted++]), 10);
        return true;
    } else if (getSweepName() == "logUniformIntDistribution20mValuesCardinalitySweepFixedMaxClustered1k") {
        generateUniformDistributionInMemoryWithSetCardinalityClustered(data, getNumElements(), 20*1000*1000, static_cast<int>(inputs[runsCompleted++]), 1000);
        return true;
    } else if (getSweepName() == "logUniformIntDistribution20mValuesCardinalitySweepFixedMaxClustered100k") {
        generateUniformDistributionInMemoryWithSetCardinalityClustered(data, getNumElements(), 20*1000*1000, static_cast<int>(inputs[runsCompleted++]), 100000);
        return true;
    } else if (getSweepName() == "logUniformIntDistribution40mValuesCardinalitySweepFixedMax") {
        generateUniformDistributionInMemoryWithSetCardinality(data, getNumElements(), 40*1000*1000, static_cast<int>(inputs[runsCompleted++]));
        return true;
    } else if (getSweepName() == "linearUniformIntDistribution20mValuesCardinalitySections_100_10m_Max20m") {
        generateUniformDistributionInMemoryWithTwoCardinalitySections(data, getNumElements(), 20*1000*1000, 100, 10*1000*1000, inputs[runsCompleted++]);
        return true;
    } else if (getSweepName() == "linearUniformIntDistribution200mValuesCardinalitySections_100_10m_Max20m") {
        generateUniformDistributionInMemoryWithTwoCardinalitySections(data, getNumElements(), 20*1000*1000, 100, 10*1000*1000, inputs[runsCompleted++]);
        return true;
    } else if (getSweepName() == "linearUniformIntDistribution200mValuesCardinalitySections_100_100m_Max200m") {
        generateUniformDistributionInMemoryWithTwoCardinalitySections(data, getNumElements(), 200*1000*1000, 100, 100*1000*1000, inputs[runsCompleted++]);
        return true;
    } else if (getSweepName() == "linearUniformIntDistribution20mValuesCardinalitySections_100_3m_Max20m") {
        generateUniformDistributionInMemoryWithTwoCardinalitySections(data, getNumElements(), 20*1000*1000, 100, 3*1000*1000, inputs[runsCompleted++]);
        return true;
    } else if (getSweepName() == "linearUniformIntDistribution20mValuesCardinalitySections_3m_100_Max20m") {
        generateUniformDistributionInMemoryWithTwoCardinalitySections(data, getNumElements(), 20*1000*1000, 3*1000*1000, 100, inputs[runsCompleted++]);
        return true;
    } else if (getSweepName() == "linearUniformIntDistribution20mValuesMultipleCardinalitySections_100_3m_Max20m") {
        generateUniformDistributionInMemoryWithMultipleTwoCardinalitySections(data, getNumElements(), 20*1000*1000, 100, 3*1000*1000, inputs[runsCompleted++], 5);
        return true;
    } else if (getSweepName() == "linearUniformIntDistribution20mValuesMultipleCardinalitySections_3m_100_Max20m") {
        generateUniformDistributionInMemoryWithMultipleTwoCardinalitySections(data, getNumElements(), 20*1000*1000, 3*1000*1000, 100, inputs[runsCompleted++], 5);
        return true;
    } else if (getSweepName() == "linearUniformIntDistribution200mValuesMultipleCardinalitySections_100_10m_Max100m") {
        generateUniformDistributionInMemoryWithMultipleTwoCardinalitySections(data, getNumElements(), 100*1000*1000, 100, 10*1000*1000, inputs[runsCompleted++], 5);
        return true;
    } else if (getSweepName() == "linearUniformIntDistribution200mValuesMultipleCardinalitySections_10m_100_Max100m") {
        generateUniformDistributionInMemoryWithMultipleTwoCardinalitySections(data, getNumElements(), 100*1000*1000, 10*1000*1000, 100, inputs[runsCompleted++], 5);
        return true;
    }
    return false;
}

bool DataSweep::loadNextDataSetIntoMemory(int64_t *data) {
    if (runsCompleted == totalRuns) {
        return false;
    }

    if (getSweepName() == "logUniformInt64Distribution200mValuesCardinalitySweepFixedMax") {
        generateUniformDistributionInMemoryWithSetCardinality(data, getNumElements(), 200*1000*1000, static_cast<int>(inputs[runsCompleted++]));
        return true;
    } else if (getSweepName() == "logUniformInt64Distribution20mValuesCardinalitySweepFixedMax") {
        generateUniformDistributionInMemoryWithSetCardinality(data, getNumElements(), 20*1000*1000, static_cast<int>(inputs[runsCompleted++]));
        return true;
    }
    return false;
}

int DataSweep::getCardinality() const {
    if (getSweepName() == "logUniformIntDistribution200mValuesCardinalitySweepVariableMax" ||
        getSweepName() == "logUniformIntDistribution200mValuesCardinalitySweepFixedMax" ||
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
        getSweepName() == "logUniformIntDistribution20mValuesCardinalitySweepFixedMax" ||
        getSweepName() == "logUniformInt64Distribution200mValuesCardinalitySweepFixedMax" ||
        getSweepName() == "logUniformInt64Distribution20mValuesCardinalitySweepFixedMax") {
        return static_cast<int>(inputs[runsCompleted]);
    } else if (getSweepName() == "linearUniformIntDistribution20mValuesCardinalitySections_100_10m_Max20m" ||
        getSweepName() == "linearUniformIntDistribution200mValuesCardinalitySections_100_10m_Max20m") {
        return 10*1000*1000;
    } else if (getSweepName() == "linearUniformIntDistribution200mValuesCardinalitySections_100_100m_Max200m") {
        return 100*1000*1000;
    } else if (getSweepName() == "linearUniformIntDistribution20mValuesCardinalitySections_100_3m_Max20m" ||
        getSweepName() == "linearUniformIntDistribution20mValuesCardinalitySections_3m_100_Max20m") {
        return 3*1000*1000;
    }  else if (getSweepName() == "linearUniformIntDistribution200mValuesMultipleCardinalitySections_100_10m_Max100m"  ||
                getSweepName() == "linearUniformIntDistribution200mValuesMultipleCardinalitySections_10m_100_Max100m") {
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


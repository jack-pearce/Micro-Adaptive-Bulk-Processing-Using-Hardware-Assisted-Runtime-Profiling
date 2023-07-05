#include <utility>

#include "dataFiles.h"
#include "dataGenerators.h"
#include "../utils/dataHelpers.h"

const std::string selectCyclesFolder = "select_cycles_benchmark/";
const std::string groupByCyclesFolder = "groupBy_cycles_benchmark/";
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
        generateUnequalLowerStepSelectivityInMemory(data, getNumElements(), 51, 10, 10);
    } else if (getFileName() == "bestCaseValuesTunedUnequalLowerStep50IntDistribution250mValues") {
        generateUnequalLowerStepSelectivityInMemory(data, getNumElements(), 51, 10, 15);
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
        30,
        250*1000*1000,
        "lowerStep50IntDistribution250mValuesSweep",
        "250m int values where max value is 100 and min value is either 1 or 51"
        "Linear input of number of discrete sections from 2 to 10,000"};

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
        31,
        200*1000*1000,
        "linearUniformIntDistribution200mValuesCardinalitySweepFixedMaxCrossOverPoint",
        "Linear distribution of cardinality for 200m ints in the groupBy cross over range"};

DataSweep DataSweeps::logUniformIntDistribution400mValuesCardinalitySweepFixedMax {
        30,
        400*1000*1000,
        "logUniformIntDistribution400mValuesCardinalitySweepFixedMax",
        "Log distribution of cardinality for 400m ints from 1 to 100m. The max value is fixed at 200m, "
        "so the distribution is sparse i.e. there are gaps."};


DataSweep::DataSweep(int _totalRuns, int _numElements, std::string _sweepName, std::string _longDescription)
        : totalRuns(_totalRuns), numElements(_numElements), runsCompleted(0), sweepName(std::move(_sweepName)),
        longDescription(std::move(_longDescription)) {
    if (getSweepName() == "logSortedIntDistribution25kValuesRandomnessSweep" ||
        getSweepName() == "logSortedIntDistribution250mValuesRandomnessSweep") {
        generateLogDistribution(getTotalRuns(), 0.01, 100, inputs);
    } else if (getSweepName() == "varyingIntDistribution250mValuesSweep" ||
               getSweepName() == "lowerStep50IntDistribution250mValuesSweep") {
        generateLogDistribution(getTotalRuns(), 2, 10000, inputs);
    } else if (getSweepName() == "logUniformIntDistribution200mValuesCardinalitySweepVariableMax" ||
            getSweepName() == "logUniformIntDistribution200mValuesCardinalitySweepFixedMax" ||
            getSweepName() == "logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered1"  ||
            getSweepName() == "logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered1k"  ||
            getSweepName() == "logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered100k") {
        generateLogDistribution(getTotalRuns(), 1, getNumElements() / 2.0, inputs);
    } else if (getSweepName() == "linearUniformIntDistribution200mValuesCardinalitySweepFixedMaxCrossOverPoint") {
        generateLinearDistribution(getTotalRuns(), 1000000, 4000000, inputs);
    } else if (getSweepName() == "logUniformIntDistribution400mValuesCardinalitySweepFixedMax") {
        generateLogDistribution(getTotalRuns(), 1, 100000000, inputs);
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
    } else if (getSweepName() == "logUniformIntDistribution200mValuesCardinalitySweepVariableMax") {
        generateUniformDistributionInMemory(data, getNumElements(), static_cast<int>(inputs[runsCompleted++]));
        return true;
    } else if (getSweepName() == "logUniformIntDistribution200mValuesCardinalitySweepFixedMax") {
        generateUniformDistributionInMemoryWithSetCardinality(data, getNumElements(), getNumElements(), static_cast<int>(inputs[runsCompleted++]));
        return true;
    } else if (getSweepName() == "logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered1") {
        generateUniformDistributionInMemoryWithSetCardinalityClustered(data, getNumElements(), getNumElements(), static_cast<int>(inputs[runsCompleted++]), 1);
        return true;
    } else if (getSweepName() == "logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered1k") {
        generateUniformDistributionInMemoryWithSetCardinalityClustered(data, getNumElements(), getNumElements(), static_cast<int>(inputs[runsCompleted++]), 1000);
        return true;
    } else if (getSweepName() == "logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered100k") {
        generateUniformDistributionInMemoryWithSetCardinalityClustered(data, getNumElements(), getNumElements(), static_cast<int>(inputs[runsCompleted++]), 100000);
        return true;
    } else if (getSweepName() == "linearUniformIntDistribution200mValuesCardinalitySweepFixedMaxCrossOverPoint") {
        generateUniformDistributionInMemoryWithSetCardinality(data, getNumElements(), getNumElements(), static_cast<int>(inputs[runsCompleted++]));
        return true;
    } else if (getSweepName() == "logUniformIntDistribution400mValuesCardinalitySweepFixedMax") {
        generateUniformDistributionInMemoryWithSetCardinality(data, getNumElements(), 200000000, static_cast<int>(inputs[runsCompleted++]));
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


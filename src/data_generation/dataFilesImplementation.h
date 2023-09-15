#ifndef MABPL_DATAFILESIMPLEMENTATION_H
#define MABPL_DATAFILESIMPLEMENTATION_H

#include <utility>

#include "dataGenerators.h"
#include "dataFiles.h"


template <typename T>
void DataFile::loadDataIntoMemory(T *data) const {
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
        generatePartiallySortedInMemoryOneToOneHundred(data, getNumElements(), 100, 0);
    } else if (getFileName() == "veryNearlyFullySortedIntDistribution250mValues") {
        generatePartiallySortedInMemoryOneToOneHundred(data, getNumElements(), 100, 0.01);
    } else if (getFileName() == "nearlyFullySortedIntDistribution250mValues") {
        generatePartiallySortedInMemoryOneToOneHundred(data, getNumElements(), 100, 1);
    } else if (getFileName() == "partiallySortedIntDistribution250mValues") {
        generatePartiallySortedInMemoryOneToOneHundred(data, getNumElements(), 100, 5);
    } else if (getFileName() == "slightlySortedIntDistribution250mValues") {
        generatePartiallySortedInMemoryOneToOneHundred(data, getNumElements(), 100, 25);
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
    } else if (getFileName() == "uniformIntDistribution250mValuesMax250m") {
        generateUniformDistributionInMemory(data, getNumElements(), 268435455); // (2^28)-1
    } else if (getFileName() == "fullySortedIntDistribution250mValuesMax250m") {
        generateFullySortedUniqueInMemory(data, getNumElements());
    } else if (getFileName() == "uniqueRandom250mInt") {
        generateUniqueValuesRandomisedInMemory(data, getNumElements());
    } else if (getFileName() == "uniformIntDistribution20mValuesMax20m") {
        generateUniformDistributionInMemory(data, getNumElements(), 33554431); // (2^25)-1
    } else if (getFileName() == "fullySortedIntDistribution20mValuesMax20m") {
        generateFullySortedUniqueInMemory(data, getNumElements());
    } else if (getFileName() == "slightlyClusteredDistribution250mValuesCardinality10mMax250m") {
        generateUniformDistributionInMemoryWithSetCardinalityClustered(data, getNumElements(), getNumElements(), 10*1000*1000, 1000);
    } else if (getFileName() == "Clustered1mDistribution250mValuesCardinality10mMax250m") {
        generateUniformDistributionInMemoryWithSetCardinalityClustered(data, getNumElements(), getNumElements(), 10*1000*1000, 10*1000*1000);
    } else if (getFileName() == "uniformIntDistribution200mValuesCardinality26Max200m") {
        generateUniformDistributionInMemoryWithSetCardinality(data, getNumElements(), getNumElements(), 26);
    }
}

template <typename T>
bool DataSweep::loadNextDataSetIntoMemory(T *data) {
    if (runsCompleted == totalRuns) {
        return false;
    }

    if (getSweepName() == "logSortedIntDistribution25kValuesRandomnessSweep") {
        generatePartiallySortedInMemoryOneToOneHundred(data, getNumElements(), 10, inputs[runsCompleted++]);
        return true;
    } else if (getSweepName() == "logSortedIntDistribution250mValuesRandomnessSweep") {
        generatePartiallySortedInMemoryOneToOneHundred(data, getNumElements(), 10, inputs[runsCompleted++]);
        return true;
    } else if (getSweepName() == "varyingIntDistribution250mValuesSweep") {
        generateVaryingSelectivityInMemory(data, getNumElements(), 51, static_cast<int>(inputs[runsCompleted++]));
        return true;
    } else if (getSweepName() == "lowerStep50IntDistribution250mValuesSweep") {
        generateLowerStepSelectivityInMemory(data, getNumElements(), 51, static_cast<int>(inputs[runsCompleted++]));
        return true;
    } else if (getSweepName() == "lowerStep50IntDistribution250mValuesSweepSectionLength_1") {
        generateLowerStepSelectivityInMemoryLengthOfSection(data, getNumElements(), 51, static_cast<int>(inputs[runsCompleted++]));
        return true;
    } else if (getSweepName() == "lowerStep50IntDistribution250mValuesSweepSectionLength_2") {
        generateLowerStepSelectivityInMemoryLengthOfSection(data, getNumElements(), 51, static_cast<int>(inputs[runsCompleted++]));
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
    } else if (getSweepName() == "logUniformIntDistribution200mValuesCardinalityUpTo10mSweepFixedMax") {
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
    } else if (getSweepName() == "logUniformIntDistribution20mValuesClusteredSweepFixedCardinality1m") {
        generateUniformDistributionInMemoryWithSetCardinalityClustered(data, getNumElements(), 20*1000*1000, 1*1000*1000, inputs[runsCompleted++]);
        return true;
    } else if (getSweepName() == "logUniformIntDistribution200mValuesClusteredSweepFixedCardinality10mMax200m") {
        generateUniformDistributionInMemoryWithSetCardinalityClustered(data, getNumElements(), 200*1000*1000, 10*1000*1000, inputs[runsCompleted++]);
        return true;
    } else if (getSweepName() == "linearUniqueIntDistribution250mValuesSortednessSweep") {
        generatePartiallySortedUniqueInMemory(data, getNumElements(), inputs[runsCompleted++]);
        return true;
    } else if (getSweepName() == "logUniformIntDistribution250mValuesClusteredSweepFixedCardinality10mMax250m") {
        generateUniformDistributionInMemoryWithSetCardinalityClustered(data, getNumElements(), getNumElements(), 10*1000*1000, inputs[runsCompleted++]);
        return true;
    } else if (getSweepName() == "linearUniformIntDistribution250mValuesClusteredSweepFixedCardinality10mMax250m") {
        generateUniformDistributionInMemoryWithSetCardinalityClustered(data, getNumElements(), getNumElements(), 10*1000*1000, inputs[runsCompleted++]);
        return true;
    } else if (getSweepName() == "logUniformIntDistribution200mValuesMaxValueSweep") {
        generateUniformDistributionInMemory(data, getNumElements(), inputs[runsCompleted++]);
        return true;
    }
    return false;
}


#endif //MABPL_DATAFILESIMPLEMENTATION_H

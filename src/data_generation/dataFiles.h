#ifndef MABPL_DATAFILES_H
#define MABPL_DATAFILES_H

#include <string>
#include <vector>

extern const std::string selectCyclesFolder;
extern const std::string groupByCyclesFolder;
extern const std::string dataFilesFolder;


class DataFile {
public:
    DataFile(int _numElements, std::string _fileName, std::string _longDescription);

    [[nodiscard]] int getNumElements() const;
    [[nodiscard]] const std::string& getFileName() const;
    [[nodiscard]] const std::string& getLongDescription() const;
    void loadDataIntoMemory(int *data) const;

private:
    int numElements;
    std::string fileName;
    std::string longDescription;
};

class DataFiles {
public:
    static const DataFile uniformIntDistribution25kValuesMax100;
    static const DataFile uniformIntDistribution250mValuesMax100;
    static const DataFile uniformIntDistribution250mValuesMax10000;
    static const DataFile uniformIntDistribution250mValuesMax1000000;

    static const DataFile varyingIntDistribution25kValues;
    static const DataFile varyingIntDistribution250mValues;

    static const DataFile upperStep50IntDistribution25kValues;
    static const DataFile upperStep50IntDistribution250mValues;
    static const DataFile worstCaseIndexesTunedUpperStep50IntDistribution250mValues;

    static const DataFile worstCaseValuesTunedLowerStep50IntDistribution250mValues;
    static const DataFile bestCaseIndexesTunedUnequalLowerStep50IntDistribution250mValues;
    static const DataFile bestCaseValuesTunedUnequalLowerStep50IntDistribution250mValues;

    static const DataFile fullySortedIntDistribution250mValues;
    static const DataFile veryNearlyFullySortedIntDistribution250mValues;
    static const DataFile nearlyFullySortedIntDistribution250mValues;
    static const DataFile partiallySortedIntDistribution250mValues;
    static const DataFile slightlySortedIntDistribution250mValues;

    static const DataFile uniformIntDistribution200mValuesCardinality400kMax200m;
    static const DataFile uniformIntDistribution200mValuesCardinality4175320Max200mClustered1k;
    static const DataFile uniformIntDistribution20mValuesCardinality400kMax20m;
    static const DataFile uniformIntDistribution20mValuesCardinality10mMax20mClustered1k;
    static const DataFile uniformIntDistribution20mValuesCardinality10mMax20m;
    static const DataFile uniformIntDistribution20mValuesCardinality200kMax20m;
    static const DataFile uniformIntDistribution20mValuesCardinality50kMax20m;

    static const DataFile uniformIntDistribution20mValuesTwo10mCardinalitySections_100_10m_Max20m;
};

class DataSweep {
public:
    DataSweep(int _totalRuns, int _numElements, std::string _sweepName, std::string _longDescription);

    [[nodiscard]] int getTotalRuns() const;
    [[nodiscard]] int getNumElements() const;
    [[nodiscard]] const std::string& getSweepName() const;
    [[nodiscard]] const std::string& getLongDescription() const;
    [[nodiscard]] int getCardinality() const;
    [[nodiscard]] float getRunInput() const;
    bool loadNextDataSetIntoMemory(int *data);
    bool loadNextDataSetIntoMemory(int64_t *data);
    void restartSweep();

private:
    int totalRuns;
    int numElements;
    int runsCompleted;
    std::string sweepName;
    std::string longDescription;
    std::vector<float> inputs;
};

class DataSweeps {
public:
    static DataSweep logSortedIntDistribution25kValuesRandomnessSweep;
    static DataSweep logSortedIntDistribution250mValuesRandomnessSweep;

    static DataSweep varyingIntDistribution250mValuesSweep;

    static DataSweep lowerStep50IntDistribution250mValuesSweep;

    static DataSweep lowerStep50IntDistribution250mValuesPercentageStepSweep;

    static DataSweep logUniformIntDistribution200mValuesCardinalitySweepVariableMax;
    static DataSweep logUniformIntDistribution200mValuesCardinalitySweepFixedMax;
    static DataSweep logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered1;
    static DataSweep logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered1k;
    static DataSweep logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered100k;

    static DataSweep linearUniformIntDistribution200mValuesCardinalitySweepFixedMaxCrossOverPoint;

    static DataSweep logUniformIntDistribution400mValuesCardinalitySweepFixedMax;
    static DataSweep logUniformInt64Distribution200mValuesCardinalitySweepFixedMax;

    static DataSweep logUniformIntDistribution20mValuesCardinalitySweepVariableMax;
    static DataSweep logUniformIntDistribution20mValuesCardinalitySweepFixedMax;
    static DataSweep logUniformIntDistribution20mValuesCardinalitySweepFixedMaxClustered10;
    static DataSweep logUniformIntDistribution20mValuesCardinalitySweepFixedMaxClustered1k;
    static DataSweep logUniformIntDistribution20mValuesCardinalitySweepFixedMaxClustered100k;

    static DataSweep logUniformIntDistribution40mValuesCardinalitySweepFixedMax;
    static DataSweep logUniformInt64Distribution20mValuesCardinalitySweepFixedMax;

    static DataSweep linearUniformIntDistribution20mValuesCardinalitySections_100_10m_Max20m;
    static DataSweep linearUniformIntDistribution200mValuesCardinalitySections_100_10m_Max20m;
    static DataSweep linearUniformIntDistribution200mValuesCardinalitySections_100_100m_Max200m;
    static DataSweep linearUniformIntDistribution20mValuesCardinalitySections_100_3m_Max20m;
    static DataSweep linearUniformIntDistribution20mValuesCardinalitySections_3m_100_Max20m;

    static DataSweep linearUniformIntDistribution20mValuesMultipleCardinalitySections_100_3m_Max20m;
    static DataSweep linearUniformIntDistribution20mValuesMultipleCardinalitySections_3m_100_Max20m;
    static DataSweep linearUniformIntDistribution200mValuesMultipleCardinalitySections_100_10m_Max100m;
    static DataSweep linearUniformIntDistribution200mValuesMultipleCardinalitySections_10m_100_Max100m;

};

#endif //MABPL_DATAFILES_H

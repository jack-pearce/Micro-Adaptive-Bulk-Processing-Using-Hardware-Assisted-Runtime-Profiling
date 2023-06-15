#ifndef MABPL_DATAFILES_H
#define MABPL_DATAFILES_H

#include <string>

extern const std::string inputFilePath;
extern const std::string outputFilePath;
extern const std::string selectCyclesFolder;


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
    static const DataFile uniformIntDistribution25kValues;
    static const DataFile uniformIntDistribution250mValues;

    static const DataFile varyingIntDistribution25kValues;
    static const DataFile varyingIntDistribution250mValues;

    static const DataFile step50IntDistribution25kValues;
    static const DataFile step50IntDistribution250mValues;
};

#endif //MABPL_DATAFILES_H

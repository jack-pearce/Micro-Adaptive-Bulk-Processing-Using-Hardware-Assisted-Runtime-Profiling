#ifndef MABPL_DATAFILES_H
#define MABPL_DATAFILES_H

#include <string>

extern const std::string outputFilePath;
extern const std::string selectCyclesFolder;


class DataFile {
public:
    DataFile(std::string _filePath, int _numElements, std::string _fileName);

    [[nodiscard]] const std::string& getFilepath() const;
    [[nodiscard]] int getNumElements() const;
    [[nodiscard]] const std::string& getFileName() const;

private:
    std::string filePath;
    int numElements;
    std::string fileName;
};

class DataFiles {
public:
    static const DataFile uniformIntDistribution25kValues;
    static const DataFile uniformIntDistribution250mValues;
};

#endif //MABPL_DATAFILES_H

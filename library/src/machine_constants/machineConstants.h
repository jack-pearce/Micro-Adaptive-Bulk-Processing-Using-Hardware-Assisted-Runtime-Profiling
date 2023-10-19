#ifndef HAQP_MACHINECONSTANTS_H
#define HAQP_MACHINECONSTANTS_H

#include <string>
#include <map>


namespace HAQP {

void printMachineConstants();
void calculateMissingMachineConstants();
void clearAndRecalculateMachineConstants();

class MachineConstants {
public:
    static MachineConstants& getInstance();
    MachineConstants(const MachineConstants&) = delete;
    void operator=(const MachineConstants&) = delete;

    double getMachineConstant(std::string &key);
    void updateMachineConstant(const std::string& key, double value);
    void printMachineConstants();
    void calculateMissingMachineConstants();
    void clearAndRecalculateMachineConstants();

private:
    MachineConstants();
    ~MachineConstants() = default;
    void loadMachineConstants(std::map<std::string, double> &map);
    void writeEmptyFile();

    std::map<std::string, double> machineConstants;
    std::string machineConstantsFilePath;
};

}

#include "machineConstantsImplementation.h"

#endif //HAQP_MACHINECONSTANTS_H

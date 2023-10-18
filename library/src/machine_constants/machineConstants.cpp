#include <iostream>
#include <fstream>
#include <json/json.h>
#include <cmath>

#include "machineConstants.h"


namespace MABPL {


void calculateMissingMachineConstants() {
    MachineConstants::getInstance().calculateMissingMachineConstants();
}

void clearAndRecalculateMachineConstants() {
    MachineConstants::getInstance().clearAndRecalculateMachineConstants();
}

void printMachineConstants() {
    MachineConstants::getInstance().printMachineConstants();
}

void calculatePartitionMachineConstants() {
    std::cout << "Calculating machine constants for Partition_minRadixBits... ";
    double minimumRadixBits = log2(static_cast<double>(l2TlbEntriesFor4KbytePages()) / 4);
    int roundedMinimumRadixBits = static_cast<int>(std::floor(minimumRadixBits));
    MachineConstants::getInstance().updateMachineConstant("Partition_minRadixBits", roundedMinimumRadixBits);
    std::cout << " Complete" << std::endl;

    std::cout << "Calculating machine constants for Partition_startRadixBits... ";
    int startRadixBits = roundedMinimumRadixBits + 8;
    MachineConstants::getInstance().updateMachineConstant("Partition_startRadixBits", startRadixBits);
    std::cout << " Complete" << std::endl;
}


MachineConstants& MachineConstants::getInstance() {
    static MachineConstants instance;
    return instance;
}

MachineConstants::MachineConstants() {
    machineConstantsFilePath = getCurrentWorkingDirectory()
            + "/data/machine_constants/machineConstantValues.json";
    loadMachineConstants(machineConstants);
}

double MachineConstants::getMachineConstant(std::string &key) {
    if (machineConstants.count(key) == 0) {
        std::cout << "Machine constant for " << key << " does not exist, calculating all missing values now. "
                                                       "This may take a while." << std::endl;
        calculateMissingMachineConstants();
    }
    return machineConstants[key];
}

void MachineConstants::updateMachineConstant(const std::string& key, double value) {
    Json::Value jsonRoot;
    std::ifstream inputFile(machineConstantsFilePath);
    if (inputFile.is_open()) {
        inputFile >> jsonRoot;
        inputFile.close();
    }

    jsonRoot[key] = value;

    std::ofstream outputFile(machineConstantsFilePath);
    if (outputFile.is_open()) {
        outputFile << jsonRoot;
        outputFile.close();
    } else {
        std::cerr << "Error opening file: " << machineConstantsFilePath << std::endl;
    }

    loadMachineConstants(machineConstants);
}

void MachineConstants::loadMachineConstants(std::map<std::string, double> &map) {
    map.clear();

    std::ifstream file(machineConstantsFilePath);
    if (file.is_open()) {
        Json::Value jsonRoot;
        file >> jsonRoot;
        file.close();

        for (const auto& key : jsonRoot.getMemberNames()) {
            map[key] = jsonRoot[key].asDouble();
        }
    } else {
        writeEmptyFile();
        map = {};
    }
}

void MachineConstants::writeEmptyFile() {
    std::ofstream file(machineConstantsFilePath, std::ofstream::out | std::ofstream::trunc);
    if (file.is_open()) {
        file << "{}";
        file.close();
    } else {
        std::cerr << "Error opening file: " << machineConstantsFilePath << std::endl;
    }
}

void MachineConstants::calculateMissingMachineConstants() {
    int dop = 1;
    while (dop <= logicalCoresCount()) {
        std::string dopStr = std::to_string(dop);

        if (machineConstants.count("SelectIndexesLower_4B_inputFilter_" + dopStr + "_dop") == 0 ||
            machineConstants.count("SelectIndexesUpper_4B_inputFilter_" + dopStr + "_dop") == 0) {
            calculateSelectIndexesMachineConstants<int>(dop);
        }
        if (machineConstants.count("SelectIndexesLower_8B_inputFilter_" + dopStr + "_dop") == 0 ||
            machineConstants.count("SelectIndexesUpper_8B_inputFilter_" + dopStr + "_dop") == 0) {
            calculateSelectIndexesMachineConstants<int64_t>(dop);
        }

        if (machineConstants.count("SelectValuesLowerMispredictions_4B_inputFilter_4B_inputData_" + dopStr + "_dop") == 0 ||
            machineConstants.count("SelectValuesLowerSelectivity_4B_inputFilter_4B_inputData_" + dopStr + "_dop") == 0) {
            calculateSelectValuesMachineConstants<int, int>(dop);
        }
        if (machineConstants.count("SelectValuesLowerMispredictions_8B_inputFilter_4B_inputData_" + dopStr + "_dop") == 0 ||
            machineConstants.count("SelectValuesLowerSelectivity_8B_inputFilter_4B_inputData_" + dopStr + "_dop") == 0) {
            calculateSelectValuesMachineConstants<int64_t, int>(dop);
        }
        if (machineConstants.count("SelectValuesLowerMispredictions_4B_inputFilter_8B_inputData_" + dopStr + "_dop") == 0 ||
            machineConstants.count("SelectValuesLowerSelectivity_4B_inputFilter_8B_inputData_" + dopStr + "_dop") == 0) {
            calculateSelectValuesMachineConstants<int, int64_t>(dop);
        }
        if (machineConstants.count("SelectValuesLowerMispredictions_8B_inputFilter_8B_inputData_" + dopStr + "_dop") == 0 ||
            machineConstants.count("SelectValuesLowerSelectivity_8B_inputFilter_8B_inputData_" + dopStr + "_dop") == 0) {
            calculateSelectValuesMachineConstants<int64_t, int64_t>(dop);
        }

        if (machineConstants.count("GroupBy_4B_inputFilter_4B_inputAggregate_" + dopStr + "_dop") == 0) {
            calculateGroupByMachineConstants<int, int>(dop);
        }
        if (machineConstants.count("GroupBy_8B_inputFilter_4B_inputAggregate_" + dopStr + "_dop") == 0) {
            calculateGroupByMachineConstants<int64_t, int>(dop);
        }
        if (machineConstants.count("GroupBy_4B_inputFilter_8B_inputAggregate_" + dopStr + "_dop") == 0) {
            calculateGroupByMachineConstants<int, int64_t>(dop);
        }
        if (machineConstants.count("GroupBy_8B_inputFilter_8B_inputAggregate_" + dopStr + "_dop") == 0) {
            calculateGroupByMachineConstants<int64_t, int64_t>(dop);
        }

        dop *= 2;
    }
    if (machineConstants.count("Partition_minRadixBits") == 0 ||
        machineConstants.count("Partition_startRadixBits") == 0) {
        calculatePartitionMachineConstants();
    }
}

void MachineConstants::clearAndRecalculateMachineConstants() {
    writeEmptyFile();
    loadMachineConstants(machineConstants);
    calculateMissingMachineConstants();
}

void MachineConstants::printMachineConstants() {
    std::cout << "Machine Constants:" << std::endl;
    for (const auto& machineConstant : machineConstants) {
        std::cout << "Constant: '" << machineConstant.first << "', Value: " << machineConstant.second << std::endl;
    }
}


}

#include <iostream>

#include "select.h"


std::string getSelectName(Select selectImplementation) {
    switch(selectImplementation) {
        case Select::ImplementationIndexesBranch:
            return "Select_Indexes_Branch";
        case Select::ImplementationIndexesPredication:
            return "Select_Indexes_Predication";
        case Select::ImplementationIndexesAdaptive:
            return "Select_Indexes_Adaptive";
        case Select::ImplementationValuesBranch:
            return "Select_Values_Branch";
        case Select::ImplementationValuesPredication:
            return "Select_Values_Predication";
        case Select::ImplementationValuesVectorized:
            return "Select_Values_Vectorized";
        case Select::ImplementationValuesAdaptive:
            return "Select_Values_Adaptive";
        default:
            std::cout << "Invalid selection of 'Select' implementation!" << std::endl;
            exit(1);
    }
}
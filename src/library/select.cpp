#include <iostream>

#include "select.h"


std::string getSelectName(SelectImplementation selectImplementation) {
    switch(selectImplementation) {
        case SelectImplementation::IndexesBranch:
            return "Select_Indexes_Branch";
        case SelectImplementation::IndexesPredication:
            return "Select_Indexes_Predication";
        case SelectImplementation::IndexesAdaptive:
            return "Select_Indexes_Adaptive";
        case SelectImplementation::ValuesBranch:
            return "Select_Values_Branch";
        case SelectImplementation::ValuesPredication:
            return "Select_Values_Predication";
        case SelectImplementation::ValuesVectorized:
            return "Select_Values_Vectorized";
        case SelectImplementation::ValuesAdaptive:
            return "Select_Values_Adaptive";
        default:
            std::cout << "Invalid selection of 'Select' implementation!" << std::endl;
            exit(1);
    }
}
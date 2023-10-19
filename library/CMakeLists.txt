link_directories("${CMAKE_SOURCE_DIR}/libs/papi/src/install/lib")

find_package(PkgConfig REQUIRED)
pkg_check_modules(JSONCPP REQUIRED jsoncpp)
include_directories(${JSONCPP_INCLUDE_DIRS})

set(RobinMap_BINARY_DIR ${CMAKE_BINARY_DIR}/libs/robin-map-build)
add_subdirectory(${CMAKE_SOURCE_DIR}/libs/robin-map ${RobinMap_BINARY_DIR})

add_library(hazard_adaptive_library
        src/operators/select.cpp
        src/utilities/papiWrapper.cpp
        src/utilities/systemInformation.cpp
        src/operators/groupBy.cpp
        src/machine_constants/machineConstants.cpp
        src/operators/partition.cpp
        include/mabpl.h
)

target_include_directories(hazard_adaptive_library
        PUBLIC
            ${CMAKE_CURRENT_SOURCE_DIR}/src
            ${CMAKE_CURRENT_SOURCE_DIR}/include
        PRIVATE
            ${CMAKE_SOURCE_DIR}/libs/papi/src/install/include
)

target_link_libraries(hazard_adaptive_library PUBLIC tsl::robin_map)
target_link_libraries(hazard_adaptive_library PRIVATE papi)
target_link_libraries(hazard_adaptive_library PRIVATE ${JSONCPP_LIBRARIES})
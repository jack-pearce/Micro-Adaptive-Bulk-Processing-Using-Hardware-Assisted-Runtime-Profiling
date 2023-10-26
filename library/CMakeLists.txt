set(LIB_NAME "hazard_adaptive_library")

link_directories("${CMAKE_SOURCE_DIR}/libs/papi/src/install/lib")

find_package(PkgConfig REQUIRED)
pkg_check_modules(JSONCPP REQUIRED jsoncpp)
include_directories(${JSONCPP_INCLUDE_DIRS})

set(RobinMap_BINARY_DIR ${CMAKE_BINARY_DIR}/libs/robin-map-build)
add_subdirectory(${CMAKE_SOURCE_DIR}/libs/robin-map ${RobinMap_BINARY_DIR})

add_library(${LIB_NAME}
        src/operators/select.cpp
        src/utilities/papiWrapper.cpp
        src/utilities/systemInformation.cpp
        src/operators/groupBy.cpp
        src/machine_constants/machineConstants.cpp
        src/operators/partition.cpp
        include/haqp.hpp
)

target_include_directories(${LIB_NAME}
        PUBLIC
            ${CMAKE_CURRENT_SOURCE_DIR}/src
            ${CMAKE_CURRENT_SOURCE_DIR}/include
        PRIVATE
            ${CMAKE_SOURCE_DIR}/libs/papi/src/install/include
)

target_link_libraries(${LIB_NAME} PUBLIC tsl::robin_map)
target_link_libraries(${LIB_NAME} PRIVATE papi)
target_link_libraries(${LIB_NAME} PRIVATE ${JSONCPP_LIBRARIES})
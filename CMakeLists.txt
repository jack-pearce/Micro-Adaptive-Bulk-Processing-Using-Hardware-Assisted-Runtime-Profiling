cmake_minimum_required(VERSION 3.0)

project(HAQP)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -g -O3 -march=native -std=c++17")

add_subdirectory(library)
add_subdirectory(benchmarks)
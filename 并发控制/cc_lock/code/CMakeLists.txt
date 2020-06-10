cmake_minimum_required(VERSION 3.16)
project(ccLock)

set(CMAKE_CXX_STANDARD 14)

set(SOURCE_FILES structure.cpp cc_lock.cpp)

add_executable(ccLock main.cpp ${SOURCE_FILES} ${SOURCES})
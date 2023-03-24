# ==========================================================================
# Copyright (C) 2023 Intel Corporation
#
# SPDX-License-Identifier: MIT
# ==========================================================================

cmake_minimum_required(VERSION 3.16.3)
project(qpl-java)
set(CMAKE_BUILD_TYPE Release)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -fPIC -std=c++17 -shared -D_FORTIFY_SOURCE=2 -Wl,-z,noexecstack -Wl,-z,relro -Wl,-z,now -Wformat -Wformat-security -Werror=format-security -fstack-protector")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -pie")

#include Java packages
FIND_PACKAGE(Java REQUIRED)
FIND_PACKAGE(JNI REQUIRED)

LINK_DIRECTORIES(/usr/local/lib64)
LINK_DIRECTORIES(/usr/local/lib)

INCLUDE_DIRECTORIES(${JAVA_INCLUDE_PATH})
INCLUDE_DIRECTORIES(${JAVA_INCLUDE_PATH2})

include_directories(include)

#Add the source files
file(GLOB SOURCES "*.cpp")

#Generate the shared library from the sources
add_library(qpl-java SHARED ${SOURCES})
#Add dependency for qpl library
target_link_libraries(qpl-java qpl)
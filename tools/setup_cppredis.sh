#!/bin/bash
source ./setup_proj.sh

cd ${PROJ_DIR}/include/cpp_redis/tacopie
# see this pr https://github.com/cpp-redis/tacopie/pull/5
#git fetch origin pull/5/head:cmake-fixes
#git checkout cmake-fixes
sudo rm -rf build
# Create a build directory and move into it
mkdir -p build && cd build
# Generate the Makefile using CMake
cmake .. -DCMAKE_BUILD_TYPE=Release
# Build the library
make
# Install the library
sudo make install

cd ${PROJ_DIR}/include/cpp_redis
# Create a build directory and move into it
sudo rm -rf build
mkdir -p build && cd build
# Generate the Makefile using CMake
cmake .. -DCMAKE_BUILD_TYPE=Release
# Build the library
make
# Install the library
sudo make install

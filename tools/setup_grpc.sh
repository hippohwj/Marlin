#!/bin/bash
source setup_conf.sh
# install grpc
# make sure no libprotobuf installed before using this script
cd ${HOME}
echo "cur dir ${HOME}"
sudo rm -rf grpc
git clone https://github.com/grpc/grpc
cd grpc
git checkout v1.51.0 
git submodule update --init
cd test/distrib/cpp/
cp ${PROJ_DIR}/tools/run_distrib_test_cmake.sh ./
./run_distrib_test_cmake.sh
export PKG_CONFIG_PATH=/usr/local/grpc/lib/pkgconfig
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH

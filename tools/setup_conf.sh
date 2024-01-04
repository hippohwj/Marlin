#!/bin/bash
# setup library path
source ./setup_proj.sh
pushd /etc/ld.so.conf.d
echo "${PROJ_DIR}/src/libs/" | sudo tee -a other.conf
echo "/usr/local/lib/" | sudo tee -a other.conf
echo "/usr/lib/x86_64-linux-gnu/" | sudo tee -a other.conf
sudo /sbin/ldconfig
export PKG_CONFIG_PATH=/usr/local/grpc/lib/pkgconfig
export LD_LIBRARY_PATH=${PROJ_DIR}/src/libs:/usr/local/lib:$LD_LIBRARY_PATH
popd

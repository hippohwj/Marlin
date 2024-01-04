#! /bin/bash
basic_install() {
  source ./tools/setup_proj.sh
  sudo apt-get update -y
  sudo apt install g++ make
  sudo apt-get install xfsprogs -y
  sudo apt-get install -y libssl-dev
  # install cmake 3.24.1
  sudo apt remove --purge --auto-remove cmake
  export version=3.24
  export build=1
  mkdir ~/temp
  cd ~/temp
  wget https://cmake.org/files/v$version/cmake-$version.$build.tar.gz
  tar -xzvf cmake-$version.$build.tar.gz
  cd cmake-$version.$build/

  ./bootstrap
  make -j$(nproc)
  sudo make install
  cd ..
  mv cmake-$version.$build cmake

  cmake --version

  echo "INFO: basic install finished!!!"
}

install_extra() { 
  source ./tools/setup_proj.sh
  #setup grpc
  cd ${PROJ_DIR}/tools
  source setup_grpc.sh
  #setup azure storage
  cd ${PROJ_DIR}/tools
  source setup_azure_sdk.sh
  #setup redis
  cd ${PROJ_DIR}/tools
  source setup_cppredis.sh
  cd ${PROJ_DIR}/tools
  source setup_conf.sh
  cd ${PROJ_DIR}
  echo "INFO: install extra finished!!!"
}

compile_repo() {
 ./compile.sh
}


if [ $1 == "install" ]
then
  basic_install
  install_extra
elif [ $1 == "compile" ]
then
  compile_repo
else
  echo "ERROR: expect the first parameter as install or compile"
fi

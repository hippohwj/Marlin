source ./setup_proj.sh

sudo apt-get update
sudo apt-get install -y libboost-tools-dev 
sudo apt-get install -y libboost-all-dev

sudo apt-get install -y libssl-dev
sudo apt-get install -y software-properties-common
sudo apt-get install -y python-software-properties
sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
wget -O - http://llvm.org/apt/llvm-snapshot.gpg.key | sudo apt-key add -
sudo add-apt-repository -y 'deb http://apt.llvm.org/xenial/ llvm-toolchain-xenial-4.0 main'
sudo apt-get install -y build-essential gcc make g++ clang clang++ lldb lld gdb
sudo apt-get -y install clang-5.0 libc++-dev
sudo apt-get install -y git flex bison libnuma-dev
sudo apt-get install -y dstat
sudo apt-get install -y vim htop
sudo apt-get install -y vagrant curl
sudo apt install -y libjemalloc-dev
sudo apt install -y openjdk-8-jre-headless
sudo apt install -y cgroup-tools
sudo apt install -y python3-pip
sudo apt install -y numactl
sudo apt install -y libgtest-dev
pip3 install --upgrade pip
pip3 install pandas
sudo apt-get -y install build-essential autoconf libtool pkg-config
sudo apt-get -y install libgflags-dev
sudo apt update
echo "set number" > ~/.vimrc

## install cmake 3.24.1
#cd $HOME
#export MY_INSTALL_DIR=${HOME}/cmake
#mkdir -p $MY_INSTALL_DIR
##wget -q -O cmake-linux.sh https://github.com/Kitware/CMake/releases/download/v3.19.6/cmake-3.19.6-Linux-x86_64.sh
##sudo sh cmake-linux.sh -- --skip-license --prefix=$MY_INSTALL_DIR
##rm cmake-linux.sh

#sudo apt remove --purge --auto-remove cmake
#version=3.24
#build=1
#wget https://cmake.org/files/v$version/cmake-$version.$build.tar.gz
#tar -xzvf cmake-$version.$build.tar.gz
#cd cmake-$version.$build/
#
#./bootstrap
#make -j$(nproc)
#sudo make install
#
#cmake --version
#
#export PATH=${HOME}/cmake/bin:$PATH

# setup a version of jemalloc with profiling enabled
cd $HOME
git clone https://github.com/jemalloc/jemalloc.git
cd jemalloc
./autogen.sh --enable-prof
make -j16
sudo make install

## set up redis
#cd
#git clone https://github.com/redis/redis.git
#cd redis
#make
#cp ${PROJ_DIR}/tools/redis.conf ./
#cd
#mkdir redis_data/
#
## start redis
#mkdir -p $HOME/redis_log
#$HOME/redis/src/redis-server $HOME/redis/redis.conf > $HOME/redis_log/redis.log &

# set up ssh key
#bash
#(echo ; echo ; echo ; echo ; echo ; echo ;echo ; echo ; echo ; echo ;) | ssh-keygen -t ed25519 -C "zguo74@wisc.edu"
#eval "$(ssh-agent -s)"
#ssh-add ~/.ssh/id_ed25519

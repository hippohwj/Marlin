source ./setup_proj.sh

sudo apt-get install -y libboost-tools-dev 
sudo apt-get install -y libboost-all-dev

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

# setup a version of jemalloc with profiling enabled
cd $HOME
git clone https://github.com/jemalloc/jemalloc.git
cd jemalloc
./autogen.sh --enable-prof
make -j16
sudo make install

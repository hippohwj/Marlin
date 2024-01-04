 # install boost
 wget https://boostorg.jfrog.io/artifactory/main/release/1.82.0/source/boost_1_82_0.tar.bz2
 tar --bzip2 -xf boost_1_82_0.tar.bz2
 cd boost_1_82_0 || exit
 ## install boost into /usr/local
 ./bootstrap.sh
 sudo ./b2 install
 wget -O vcpkg.tar.gz https://github.com/microsoft/vcpkg/archive/master.tar.gz
 sudo mkdir /opt/vcpkg
 sudo apt-get install curl zip unzip tar
 sudo tar xf vcpkg.tar.gz --strip-components=1 -C /opt/vcpkg
 sudo /opt/vcpkg/bootstrap-vcpkg.sh
 sudo ln -s /opt/vcpkg/vcpkg /usr/local/bin/vcpkg
 sudo vcpkg install cpprestsdk
 sudo vcpkg install azure-storage-blobs-cpp
 sudo vcpkg install azure-storage-cpp
 curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

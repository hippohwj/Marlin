pushd tools
source ./setup_conf.sh
popd 
rm -rf build/
mkdir -p build/
cd build/
cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_TOOLCHAIN_FILE=/opt/vcpkg/scripts/buildsystems/vcpkg.cmake ../
make -j16 ComputeService > /${PROJ_DIR}/compile_out 2>&1

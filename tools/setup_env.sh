source ./setup_proj.sh
export PKG_CONFIG_PATH=/usr/local/grpc/lib/pkgconfig
export LD_LIBRARY_PATH=${PROJ_DIR}/src/lib:/usr/local/lib:$LD_LIBRARY_PATH
export PATH=${HOME}/cmake/bin:$PATH

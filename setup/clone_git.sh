#! /bin/bash
GIT_DIR=git@github.com:hippohwj/Arboretum-Distributed.git
CONFIG_DIR=~/config

cd ~
# copy ssh key
cp ${CONFIG_DIR}/ssh/* ~/.ssh/

#clone git repo
git clone --recurse-submodules ${GIT_DIR}

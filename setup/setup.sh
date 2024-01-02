#! /bin/bash

# 0. check each node in cluster to make sure the default /dev/sdc is ssd, otherwise change it in config/mount_disk.sh
# 1. modify host.sh to include connections of all nodes
# 2. run this setup.sh script to setup ssh key and clone gitup
# 3. set up ssh shortcuts in ~/.zshrc for quick ssh connections

source ./host.sh
echo "hosts are: ${hosts}"

## cp config
#for host in ${hosts[@]}; do
#  ssh wjhu@${host} "sh -c 'rm -rf /users/wjhu/config'"
#  scp -r ../auto-scaling-env-single-node/config  wjhu@${host}:/users/wjhu/
#done
#
## mount ssd 
#for host in ${hosts[@]}; do
#  ssh wjhu@${host} "sh -c 'cd /users/wjhu/config;nohup ./mount_disk.sh >/users/wjhu/mnt_disk_out 2>&1 &'"
#done

# download git repo
#for host in ${hosts[@]}; do
#  scp  ../auto-scaling-env-single-node/clone_git.sh  wjhu@${host}:/users/wjhu/
#  //clone git repo into every node in the cluster
#  ssh -t wjhu@${host} "cd /users/wjhu/; ./clone_git.sh;"
#done


# install dependencies
#for host in ${hosts[@]}; do
#  # clone git repo into every node in the cluster
#  echo "cur host: ${host}"
#  #ssh wjhu@${host} "sh -c 'cd /users/wjhu/Arboretum-Distributed;nohup ./setup-single-node.sh 0 >/users/wjhu/setup_out 2>&1 &'"
#  ssh wjhu@${host} "sh -c 'cd /users/wjhu/Arboretum-Distributed;nohup ./setup-single-node.sh install_local >/users/wjhu/setup_out 2>&1 &'"
#done


## compile codes
#for host in ${hosts[@]}; do
#  ssh wjhu@${host} "sh -c 'cd /users/wjhu/Arboretum-Distributed;nohup ./setup-single-node.sh compile >/users/wjhu/compile_out 2>&1 &'"
#done

# install and start redis
for host in ${hosts[@]}; do
#  ssh wjhu@${host} "sh -c 'cd /users/wjhu/Arboretum-Distributed;nohup ./setup-single-node.sh redis >/users/wjhu/startredis_out 2>&1 &'"
  ssh wjhu@${host} "sh -c 'cd /users/wjhu/Arboretum-Distributed;nohup ./setup-single-node.sh start_redis >/users/wjhu/startredis_out 2>&1 &'"
done





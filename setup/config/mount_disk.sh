#!/bin/bash

#PART=/dev/sda4
PART="/dev/sdc"
MOUNTDIR=/ssd

sudo apt-get update
sudo apt-get install xfsprogs -y

#lsblk
#sudo blkid

sudo parted ${PART} --script mklabel gpt mkpart xfspart xfs 0% 100%

NEW_PART="${PART}1"
sudo mkfs.xfs ${NEW_PART}
sudo partprobe ${NEW_PART}

sudo mkdir $MOUNTDIR
sudo mount ${NEW_PART} $MOUNTDIR

UUID=`sudo blkid | grep "${NEW_PART}" | awk '{ print $2 }' | awk -F\" '{print $(NF-1)}'`
FSTAB_STR="UUID=${UUID}  ${MOUNTDIR}   xfs   defaults,nofail   1   2"

sudo bash -c "echo ${FSTAB_STR} >> /etc/fstab"


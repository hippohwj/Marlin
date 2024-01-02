#!/bin/bash
for i in `seq 1 25`
do 
 cat /dev/null > log-blobs/log$i
 #touch log_blobs/log-$i
done

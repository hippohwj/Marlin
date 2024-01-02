#!/bin/bash

export AZURE_ACCOUNTNAME="autoscale2023"
export AZURE_ACCOUNTKEY="XiBdBjYXhTuRn4LZoy5frE7c0LSTngErnTSp5WFSJPKI+BAaCBsGUfS2jXorkXoprJ5DiB8KJy4H+AStqDiCQw=="

SAS="sv=2022-11-02&ss=bfqt&srt=co&sp=rwdlacupiytfx&se=2024-08-18T23:32:47Z&st=2023-08-18T15:32:47Z&spr=https&sig=8l783GQoYpuCTf9hGBhX89NKFEEd64jcPMZdSgdPs8U%3D"

TBL_NAME="mtbl300g"


for i in `seq 0 3`
do
  container_name="log-node-$i"
  az storage container create -n ${container_name} --fail-on-exist --account-key ${AZURE_ACCOUNTKEY} --account-name ${AZURE_ACCOUNTNAME}  

  az storage blob delete-batch --account-key ${AZURE_ACCOUNTKEY} --account-name ${AZURE_ACCOUNTNAME} --source ${container_name}
  
  azcopy copy "log-blobs/*" "https://autoscale2023.blob.core.windows.net/${container_name}?${SAS}" --blob-type AppendBlob --block-blob-tier Hot
done

az storage table delete --name ${TBL_NAME} --fail-not-exist --account-key ${AZURE_ACCOUNTKEY} --account-name ${AZURE_ACCOUNTNAME}

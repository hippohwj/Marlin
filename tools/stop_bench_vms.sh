#!/bin/bash

resource_g="Autoscale"
subscription="nsf-2144588-160529"
for i in `seq 0 3`
do
  name="autoscale-bench-n$((3-$i))"
  az vm deallocate --name ${name} --no-wait --resource-group ${resource_g} --subscription ${subscription}
done

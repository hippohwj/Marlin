# Marlin 
Marlin is a lightweight cluster management mechanism for efficient autoscaling in cloud-native OLTP DBMSs with storage disaggregation. Marlin unifies storage and management of both application and system states, eliminating the need for auxiliary services such as Zookeeper or Chubby. It also provides a series of optimizations including Cache Preheat/Postheat and Fine-Grained Synchronization to reduce reconfigurations' performance impact.

## Quick Start 

Clone the repo with ```git clone --recurse-submodules ${GIT_DIR}```

1. Install Dependencies
This step installs dependencies, such as grpc, azure-storage-cpp, redis-cpp, etc.

```
./setup.sh install
```
2. Compile and Run
```
./setup.sh compile
```
expected output
```
-- Generating Compute Service:
-- [service] executable: ComputeService
-- Generating experiments:
-- Configuring done
-- Generating done
-- Build files have been written to: /home/hippo/Marlin/build
```


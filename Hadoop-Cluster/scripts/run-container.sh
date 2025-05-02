#!/bin/bash

set -e 

# The default node number is 3
N=${1:-3}

# Calculate N-1 and store in Nminus1
Nminus1=$((N-1))

echo "Resizing cluster to $Nminus1 worker nodes..."
./scripts/resize-number-worker.sh $Nminus1
if [ $? -ne 0 ]; then
    echo "Failed to resize workers. Exiting..."
    exit 1
fi

docker build -t master-official:latest ./config

echo "Starting Docker Compose services..."
docker compose -f compose-dynamic.yaml up -d
if [ $? -ne 0 ]; then
    echo "Failed to start Docker Compose services. Exiting..."
    exit 1
fi


echo "Restarting the cluster..."
docker exec -it master /bin/bash -c "su - hadoopquochuy"
if [ $? -ne 0 ]; then
    echo "Failed to restart the cluster. Exiting..."
    exit 1
fi




echo "Cluster setup completed successfully!"

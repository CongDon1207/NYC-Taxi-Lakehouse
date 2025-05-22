#!/bin/bash

set -e 

# Check if the parameter n is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <number_of_workers>"
    exit 1
fi

n=$1

# Navigate to the config-hadoop/master directory
cd config/master/config || { echo "Directory not found"; exit 1; }

# Clear the old content of the workers file and add new content
> workers
for ((i=1; i<=n; i++)); do
    echo "quochuy-worker$i" >> workers
done

echo "Updated workers file with $n workers."

# Navigate back to the root directory
cd ../../..

# Copy base compose file
cp compose.yaml compose-dynamic.yaml
if [ $? -ne 0 ]; then
    echo "Failed to copy compose.yaml. Exiting..."
    exit 1
fi

# Remove old volumes section from compose-dynamic.yaml
sed -i '/^volumes:/,$d' compose-dynamic.yaml

# Add worker services to compose-dynamic.yaml
for ((i=1; i<=n; i++)); do
    cat <<EOL >> compose-dynamic.yaml
  worker$i:
    image: worker
    container_name: worker-huy$i
    hostname: quochuy-worker$i
    volumes:
      - hdfs_datanode$i:/home/hadoopquochuy/hadoop/hadoop_data/hdfs/datanode
    networks:
      - data_network
    ports:
      - 988$i:9870
    command: /bin/bash -c "service ssh start; tail -f /dev/null"

EOL
done

# Ensure volumes section is at the end and not duplicated
cat <<EOL >> compose-dynamic.yaml

volumes:
  hdfs_namenode:
EOL

for ((i=1; i<=n; i++)); do
    echo "  hdfs_datanode$i:" >> compose-dynamic.yaml
done

echo "Updated compose-dynamic.yaml with $n worker nodes."
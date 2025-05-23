#!/bin/bash
rm -f $HIVE_HOME/lib/slf4j-reload4j-1.7.36.jar

echo "Initializing Hive Metastore schema..."
schematool -dbType mysql -initSchema --verbose || echo "Schema already exists or initialization failed."

echo "Starting Hive Metastore service..."
exec hive --service metastore -p 9083

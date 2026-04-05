#!/bin/bash

cat <<EOF > /opt/bitnami/spark/conf/log4j2.properties
rootLogger.level = error
rootLogger.appenderRef.console.ref = Console
appender.console.type = Console
appender.console.name = Console
appender.console.layout.type = PatternLayout
appender.console.layout.pattern = %p %c{1}: %m%n
EOF

echo "Waiting for $REQUIRED_DN DataNodes..."

while true; do
  CURRENT_DN=$(hdfs dfsadmin -report | grep "Live datanodes" | awk '{print $3}' | tr -d '():')
  
  if [[ -z "$CURRENT_DN" ]]; then
    CURRENT_DN=0
  fi

  if [ "$CURRENT_DN" -ge "$REQUIRED_DN" ]; then
    echo "HDFS is ready with $CURRENT_DN nodes (required: $REQUIRED_DN)."
    break
  fi

  echo "Current live nodes: $CURRENT_DN. Waiting for $REQUIRED_DN..."
  sleep 5
done

until hdfs dfsadmin -safemode wait; do
  echo "Waiting for HDFS to leave safe mode..."
  sleep 2
done

echo "Downloading dataset to HDFS..."
hdfs dfs -mkdir -p /data
hdfs dfs -rm -f /data/dataset.csv
hdfs dfs -D dfs.replication=$CURRENT_DN -put -f /data/dataset.csv /data/

echo "Starting Spark (Optimize: $OPTIMIZE)..."
spark-submit \
 --master $SPARK_MASTER_URL \
 --conf spark.ui.showConsoleProgress=false /apps/main.py \
 --optimize $OPTIMIZE \
 --datanodes $CURRENT_DN

echo "Run succeed"
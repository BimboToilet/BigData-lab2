#!/bin/bash

mkdir -p /opt/spark/conf
cat <<EOF > /opt/spark/conf/log4j2.properties
rootLogger.level = error
rootLogger.appenderRef.console.ref = Console
appender.console.type = Console
appender.console.name = Console
appender.console.layout.type = PatternLayout
appender.console.layout.pattern = %p %c{1}: %m%n
EOF

mkdir -p $HADOOP_CONF_DIR
cat <<EOF > ${HADOOP_CONF_DIR}/core-site.xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>${HADOOP_CFG_CORE_SITE_FS_DEFAULTFS}</value>
    </property>
</configuration>
EOF

echo "Testing connection to namenode:9000..."
until timeout 1 bash -c "cat < /dev/null > /dev/tcp/namenode/9000" 2>/dev/null; do
  echo "Namenode RPC port (9000) is unreachable - waiting..."
  sleep 2
done

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
hdfs dfs -D dfs.replication=$REQUIRED_DN -put -f /data/dataset.csv /data/

echo "Starting Spark (Optimize: $OPTIMIZE)..."
spark-submit \
 --master $SPARK_MASTER_URL \
 --conf spark.ui.showConsoleProgress=false /apps/main.py \
 --optimize $OPTIMIZE \
 --datanodes $REQUIRED_DN

echo "Run succeed"
import time
import os
import argparse
import datetime
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def get_executors_metrics(spark):
    sc = spark.sparkContext
    mem_status = sc._jsc.sc().getExecutorMemoryStatus()
    
    mem_dict = {}
    iterator = mem_status.iterator()
    while iterator.hasNext():
        item = iterator.next()
        mem_dict[item._1()] = item._2()
    
    executors_count = len(mem_dict)
    
    total_used_mem_bytes = 0
    for host, mem_info in mem_dict.items():
        max_mem = mem_info._1()
        rem_mem = mem_info._2()
        total_used_mem_bytes += (max_mem - rem_mem)
        
    return executors_count, total_used_mem_bytes / (1024 * 1024)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--optimize", type=str, default="false")
    parser.add_argument("--datanodes", type=str, default="unknown")
    args = parser.parse_args()
    
    optimize = args.optimize.lower() == "true"
    dn_count = args.datanodes

    app_name = f"Experiment_DN_{dn_count}_OPT_{optimize}"

    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    start_time = time.time()

    df = spark.read.csv("hdfs:///data/dataset.csv", header=True, inferSchema=True)

    if optimize:
        df = df.repartition(6).cache() 
        df.count()

    result = df.groupBy("category_col") \
        .agg(
            F.avg("numeric_col").alias("avg_val"),
            F.countDistinct("id_col").alias("unique_count"),
            F.max("numeric_col2").alias("max_val")
        ) \
        .orderBy("avg_val", ascending=False)

    result.show()

    exec_count, total_exec_ram = get_executors_metrics(spark)
    
    end_time = time.time()
    duration = end_time - start_time

    results_path = "/data/experiments_log.csv"
    if not os.path.exists(results_path):
        with open(results_path, "w") as f:
            f.write("timestamp,dn_count,optimized,exec_count,time_sec,total_exec_ram_mb\n")

    with open(results_path, "a") as f:
        f.write(f"{datetime.datetime.now()},{dn_count},{optimize},{exec_count},{duration:.2f},{total_exec_ram:.2f}\n")

    spark.stop()

if __name__ == "__main__":
    main()
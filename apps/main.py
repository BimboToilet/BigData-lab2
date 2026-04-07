import time
import os
import argparse
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType, TimestampType, BooleanType
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

    schema = StructType([
        StructField("id_col", LongType(), False),
        StructField("category_col", StringType(), True),
        StructField("numeric_col", DoubleType(), True),
        StructField("numeric_col2", IntegerType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("is_active", BooleanType(), True)
    ])

    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.executor.memory", "512m") \
        .getOrCreate()
    
    sc = spark.sparkContext

    target_partitions = sc.defaultParallelism * 3

    sc.setJobGroup("load", "Reading CSV from HDFS")
    
    t0 = time.time()
    df = spark.read.csv("hdfs:///data/dataset.csv", header=True, schema=schema, inferSchema=False)
    load_time = time.time() - t0
    
    t1 = time.time()
    if optimize:
        df = df.repartition(target_partitions).cache() 
        df.count()
    opt_time = time.time() - t1
    
    sc.setJobGroup("process", "Grouping and Aggregation")
    
    t2 = time.time()
    result = df.groupBy("category_col") \
        .agg(
            F.avg("numeric_col").alias("avg_val"),
            F.countDistinct("id_col").alias("unique_count"),
            F.max("numeric_col2").alias("max_val")
        ) \
        .orderBy("avg_val", ascending=False)
    
    result.show()
    exec_count, total_exec_ram = get_executors_metrics(spark)
    
    calc_time = time.time() - t2

    total_time = time.time() - t0
    
    results_path = "/data/experiments_log.csv"
    if not os.path.exists(results_path):
        with open(results_path, "w") as f:
            f.write("experiment,timestamp,dn_count,optimized,exec_count,total_time_sec,calc_time_sec,opt_time_sec,load_time_sec,total_exec_ram_mb\n")

    with open(results_path, "a") as f:
        f.write(f"{app_name},{datetime.datetime.now()},{dn_count},{optimize},{exec_count},{total_time:.2f},{calc_time:.2f},{opt_time:.2f},{load_time:.2f},{total_exec_ram:.2f}\n")

    spark.stop()

if __name__ == "__main__":
    main()
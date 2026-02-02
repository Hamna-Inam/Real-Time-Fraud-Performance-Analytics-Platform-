#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ArchiveJob")

spark = SparkSession.builder \
    .appName("MongoToHDFSArchiveJob") \
    .config("spark.mongodb.read.connection.uri", "mongodb://mongo:27017") \
    .config("spark.mongodb.write.connection.uri", "mongodb://mongo:27017") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

logger.info("🔥 Spark Session Started - Archiving Job")

# -------- READ CURRENT LIVE DATA --------
df = spark.read.format("mongo") \
    .option("uri", "mongodb://mongo:27017/bda_project.transactions_fact") \
    .load()

if df.rdd.isEmpty():
    logger.info("⚠️ No live data found. Exiting archive job.")
    spark.stop()
    exit(0)

logger.info(f"📦 Loaded {df.count()} live rows")

# -------- WRITE TO HDFS (FULL SNAPSHOT) --------
batch_id = int((spark.sparkContext._jsc.sc().startTime())/1000)
output_path = f"hdfs://namenode:8020/archive/transactions_batch_{batch_id}"

logger.info(f"💾 Writing archive to {output_path}")
df.write.mode("overwrite").parquet(output_path)
logger.info("✅ Archive written to HDFS")

# -------- KEEP ONLY LAST 3 HOURS IN MONGO --------
logger.info("🗑️ Retaining only last 3 hours in Mongo")

cutoff_time = (datetime.utcnow() - timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")
logger.info(f"⏳ Cutoff = {cutoff_time}")

df_filtered = df.filter(col("transaction_timestamp") >= cutoff_time)

remaining = df_filtered.count()
logger.info(f"✅ Rows to keep in Mongo: {remaining}")

df_filtered.write.format("mongo") \
    .option("uri", "mongodb://mongo:27017/bda_project.transactions_fact") \
    .mode("overwrite") \
    .save()

logger.info("🎯 Mongo trimmed successfully (last 3 hours retained)")
spark.stop()

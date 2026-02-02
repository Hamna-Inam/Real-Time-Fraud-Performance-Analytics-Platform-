#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date
import logging
import json
import redis

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KPIJob")

# ---------------- REDIS ----------------
redis_client = redis.Redis(host="redis-cache", port=6379, decode_responses=True)

# ---------------- SPARK ----------------
spark = SparkSession.builder \
    .appName("KPIAnalyticsJob") \
    .config("spark.mongodb.read.connection.uri", "mongodb://mongo:27017") \
    .config("spark.mongodb.write.connection.uri", "mongodb://mongo:27017") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

logger.info("✓ Spark Session started")

# -------- LOAD FACT --------
df_live = spark.read.format("mongo") \
    .option("uri", "mongodb://mongo:27017/bda_project.transactions_fact") \
    .load()

df_archive = None
try:
    df_archive = spark.read.parquet("hdfs://namenode:8020/archive/transactions_batch_*/")
    logger.info("✓ Loaded archive data")
except Exception:
    logger.info("ℹ️ No archive data found yet")

if df_live.rdd.isEmpty() and (df_archive is None or df_archive.rdd.isEmpty()):
    logger.info("⚠️ No data available. Exiting.")
    spark.stop()
    exit(0)

df = df_live if df_archive is None or df_archive.rdd.isEmpty() else df_live.unionByName(df_archive)

df = df.withColumn(
    "txn_date",
    to_date(to_timestamp(col("transaction_timestamp"), "yyyy-MM-dd HH:mm:ss"))
)

df.createOrReplaceTempView("transactions_fact")

# -------- LOAD DIMENSIONS --------
dims = {
    "customers_dim": "customer_id",
    "merchants_dim": "merchant_id",
    "devices_dim": "device_id",
    "payment_methods_dim": "payment_method_id"
}

for table in dims.keys():
    tmp = spark.read.format("mongo") \
        .option("uri", f"mongodb://mongo:27017/bda_project.{table}") \
        .load()
    tmp.createOrReplaceTempView(table)
    logger.info(f"✓ Loaded {table}")

logger.info("✓ All dimensions loaded")

# =========================================================
#                    KPI QUERIES
# =========================================================

def cache_to_redis(key, df):
    logger.info(f"🔴 Caching {key} into Redis")

    rows = df.collect()
    json_data = []

    for row in rows:
        clean = {}
        for k, v in row.asDict().items():
            # Convert Spark date/timestamp to string
            try:
                if hasattr(v, "isoformat"):
                    clean[k] = v.isoformat()
                else:
                    clean[k] = v
            except:
                clean[k] = str(v)

        json_data.append(clean)

    redis_client.set(key, json.dumps(json_data))

# -------- GLOBAL KPI --------
global_kpis = spark.sql("""
SELECT 
    COUNT(*) AS total_transactions,
    SUM(transaction_amount) AS total_volume,
    AVG(transaction_amount) AS avg_transaction_value
FROM transactions_fact
""")

global_kpis.write.format("mongo") \
    .option("uri", "mongodb://mongo:27017/bda_project.kpis_global") \
    .mode("overwrite").save()

cache_to_redis("kpi:global", global_kpis)

# -------- DAILY KPI --------
daily_kpis = spark.sql("""
SELECT 
    txn_date,
    COUNT(*) AS transactions,
    SUM(transaction_amount) AS total_volume,
    AVG(transaction_amount) AS avg_value
FROM transactions_fact
GROUP BY txn_date
ORDER BY txn_date
""")

daily_kpis.write.format("mongo") \
    .option("uri", "mongodb://mongo:27017/bda_project.kpis_daily") \
    .mode("overwrite").save()

cache_to_redis("kpi:daily", daily_kpis)

# -------- MERCHANT KPI --------
merchant_kpis = spark.sql("""
SELECT 
    m.business_category,
    COUNT(*) AS transactions,
    SUM(t.transaction_amount) AS total_revenue,
    AVG(t.transaction_amount) AS avg_ticket_size
FROM transactions_fact t
JOIN merchants_dim m
ON t.merchant_id = m.merchant_id
GROUP BY m.business_category
ORDER BY total_revenue DESC
""")

merchant_kpis.write.format("mongo") \
    .option("uri", "mongodb://mongo:27017/bda_project.kpis_by_merchant_category") \
    .mode("overwrite").save()

cache_to_redis("kpi:merchant", merchant_kpis)

# -------- CUSTOMER KPI --------
customer_kpis = spark.sql("""
SELECT 
    c.customer_segment,
    COUNT(*) AS transactions,
    SUM(t.transaction_amount) AS total_spend,
    AVG(t.transaction_amount) AS avg_spend
FROM transactions_fact t
JOIN customers_dim c
ON t.customer_id = c.customer_id
GROUP BY c.customer_segment
ORDER BY total_spend DESC
""")

customer_kpis.write.format("mongo") \
    .option("uri", "mongodb://mongo:27017/bda_project.kpis_by_customer_segment") \
    .mode("overwrite").save()

cache_to_redis("kpi:customer", customer_kpis)

# -------- PAYMENT KPI --------
payment_kpis = spark.sql("""
SELECT 
    p.method_type,
    COUNT(*) AS transactions,
    SUM(t.transaction_amount) AS total_volume,
    AVG(t.transaction_amount) AS avg_value
FROM transactions_fact t
JOIN payment_methods_dim p
ON t.payment_method_id = p.payment_method_id
GROUP BY p.method_type
ORDER BY total_volume DESC
""")

payment_kpis.write.format("mongo") \
    .option("uri", "mongodb://mongo:27017/bda_project.kpis_by_payment_method") \
    .mode("overwrite").save()

cache_to_redis("kpi:payment", payment_kpis)

logger.info("🎯 ALL KPIs GENERATED + REDIS CACHED")
spark.stop()

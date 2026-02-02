import time
import random
import pandas as pd
import uuid
import os
from faker import Faker
from sdv.single_table import CTGANSynthesizer
from sdv.metadata import SingleTableMetadata
from pymongo import MongoClient

# --- CONFIGURATION ---
MONGO_URI = "mongodb://mongo:27017/"
DB_NAME = "bda_project"
COLLECTION_NAME = "transactions_fact"
REAL_TIME_DELAY = 1.0  # Generate 1 transaction every second

# Connect to Mongo (Retry logic for Docker startup)
client = None
for i in range(10):
    try:
        client = MongoClient(MONGO_URI)
        client.admin.command('ping')
        print("✅ Connected to MongoDB")
        break
    except Exception as e:
        print(f"Waiting for Mongo... ({e})")
        time.sleep(5)

db = client[DB_NAME]
collection = db[COLLECTION_NAME]

fake = Faker()

# --- STEP 1: DEFINE CONTEXT (Dimensions) ---
# We need these IDs to link the Fact table to Dimension tables
CUSTOMER_IDS = list(range(1000, 1050))
MERCHANT_IDS = list(range(2000, 2020))
DEVICE_IDS = list(range(3000, 3030))

print("--- Training GAN Model (This takes ~30 seconds) ---")

# --- STEP 2: CREATE SEED DATA FOR GAN ---
# We manually create patterns so the GAN learns "Business Logic"
seed_data = []
for _ in range(200):
    # Pattern A: Normal User (Low Amount, Fast Time, Low Fraud)
    if random.random() < 0.8:
        seed_data.append({
            "transaction_amount": random.uniform(10, 500),
            "processing_time": random.uniform(50, 200),
            "fraud_score": random.uniform(0, 0.2),
            "status": "Success"
        })
    # Pattern B: Fraudster (High Amount, Slow Time, High Fraud)
    else:
        seed_data.append({
            "transaction_amount": random.uniform(5000, 20000),
            "processing_time": random.uniform(1000, 3000),
            "fraud_score": random.uniform(0.8, 1.0),
            "status": "Fraud"
        })

df_seed = pd.DataFrame(seed_data)

# --- STEP 3: TRAIN CTGAN ---
metadata = SingleTableMetadata()
metadata.detect_from_dataframe(df_seed)

synthesizer = CTGANSynthesizer(metadata)
synthesizer.fit(df_seed)

print("✅ GAN Model Trained! Starting Stream...")

# --- STEP 4: START STREAMING TO MONGODB ---
try:
    while True:
        # 1. Generate synthetic features using GAN
        synthetic_row = synthesizer.sample(num_rows=1)
        data = synthetic_row.iloc[0].to_dict()
        
        # 2. Add Keys & Timestamps (The "Streaming" part)
        full_record = {
            "transaction_id": str(uuid.uuid4()),
            "customer_id": random.choice(CUSTOMER_IDS),
            "merchant_id": random.choice(MERCHANT_IDS),
            "device_id": random.choice(DEVICE_IDS),
            "payment_method_id": random.choice([1, 2, 3, 4]),
            "transaction_timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            # GAN generated fields:
            "transaction_amount": float(data["transaction_amount"]),
            "processing_time": float(data["processing_time"]),
            "fraud_score": float(data["fraud_score"]),
            "status": data["status"]
        }
        
        # 3. Insert into MongoDB
        collection.insert_one(full_record)
        
        print(f" -> Streamed: {full_record['transaction_id']} | Amt: ${full_record['transaction_amount']:.2f}")
        
        time.sleep(REAL_TIME_DELAY)

except KeyboardInterrupt:
    print("Stream stopped.")

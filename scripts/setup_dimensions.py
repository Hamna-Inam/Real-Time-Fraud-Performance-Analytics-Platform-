import time
from pymongo import MongoClient
from faker import Faker
import random

MONGO_URI = "mongodb://mongo:27017/"
DB_NAME = "bda_project"

fake = Faker()

# ---------------- CONNECT TO MONGO ----------------
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

if client is None:
    raise Exception("❌ Could not connect to MongoDB. Exiting.")

db = client[DB_NAME]

# ---------------- COLLECTIONS ----------------
customers = db["customers_dim"]
merchants = db["merchants_dim"]
devices = db["devices_dim"]
payments = db["payment_methods_dim"]
datetime_dim = db["datetime_dim"]


# ---------------- CUSTOMER DIM ----------------
print("➡️ Creating Customer Dimension...")
for cid in range(1000, 1050):
    customers.update_one(
        {"customer_id": cid},
        {"$set": {
            "customer_id": cid,
            "name": fake.name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "customer_segment": random.choice(["New", "Loyal", "Risky", "Occasional"]),
            "account_age_days": random.randint(30, 2000),
            "total_lifetime_spend": round(random.uniform(500, 50000), 2),
            "country": fake.country()
        }},
        upsert=True
    )

print("✅ customers_dim ready")


# ---------------- MERCHANT DIM ----------------
print("➡️ Creating Merchant Dimension...")
for mid in range(2000, 2020):
    merchants.update_one(
        {"merchant_id": mid},
        {"$set": {
            "merchant_id": mid,
            "merchant_name": fake.company(),
            "business_category": random.choice([
                "Electronics", "Fashion", "Gaming", "Travel", "Food Delivery", "Healthcare"
            ]),
            "risk_category": random.choice(["Low", "Medium", "High"]),
            "registration_date": fake.date_between(start_date="-5y", end_date="today").strftime("%Y-%m-%d"),
            "country": fake.country(),
            "avg_daily_transactions": random.randint(20, 500),
            "avg_dispute_rate": round(random.uniform(0.1, 5.0), 2)
        }},
        upsert=True
    )

print("✅ merchants_dim ready")


# ---------------- DEVICE DIM ----------------
print("➡️ Creating Device Dimension...")
for did in range(3000, 3030):
    devices.update_one(
        {"device_id": did},
        {"$set": {
            "device_id": did,
            "device_type": random.choice(["Mobile", "Desktop", "Tablet"]),
            "os": random.choice(["Android", "iOS", "Windows", "Linux", "macOS"]),
            "browser": random.choice(["Chrome", "Firefox", "Safari", "Edge"]),
            "is_rooted_jailbroken": random.choice([True, False]),
        }},
        upsert=True
    )

print("✅ devices_dim ready")


# ---------------- PAYMENT METHOD DIM ----------------
print("➡️ Creating Payment Method Dimension...")
payment_methods = [
    (1, "Credit Card"),
    (2, "Debit Card"),
    (3, "Digital Wallet"),
    (4, "Bank Transfer")
]

for pid, method in payment_methods:
    payments.update_one(
        {"payment_method_id": pid},
        {"$set": {
            "payment_method_id": pid,
            "method_type": method,
            "issuer_bank": random.choice([
                "HSBC", "Citi", "Standard Chartered", "Chase", "Bank of America", "HBL", "UBL"
            ]),
            "risk_weight": round(random.uniform(0.1, 1.0), 2)
        }},
        upsert=True
    )

print("✅ payment_methods_dim ready")


# ---------------- DATETIME DIM (Optional Lookup) ----------------
print("➡️ Creating Datetime Dimension...")
# Keeping it simple – supports joins on hour/day/weekend analytics
datetime_dim.update_one(
    {"_id": "static_time_dim"},
    {"$set": {
        "hours": list(range(0, 24)),
        "days_of_week": ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
        "weekend_flags": {"Sat": True, "Sun": True}
    }},
    upsert=True
)

print("✅ datetime_dim ready")
print("\n🎯 ALL DIMENSION TABLES CREATED SUCCESSFULLY")

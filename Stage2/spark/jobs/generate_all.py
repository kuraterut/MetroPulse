
from faker import Faker
import pandas as pd
import random
import uuid
from datetime import datetime, timedelta

fake = Faker("ru_RU")

S3_OPTS = {
    "key": "minio",
    "secret": "minio123",
    "client_kwargs": {"endpoint_url": "http://minio:9000"}
}

# ---------- USERS ----------
def gen_users(n=1000):
    return pd.DataFrame([
        {
            "user_id": i,
            "name": fake.name(),
            "email": fake.unique.email(),
            "city": fake.city(),
            "created_at": fake.date_time_this_year()
        } for i in range(1, n + 1)
    ])

# ---------- ROUTES ----------
def gen_routes():
    return pd.DataFrame([
        {"route_id": 1, "route_number": "A-10", "vehicle_type": "bus", "base_fare": 50.0},
        {"route_id": 2, "route_number": "B-20", "vehicle_type": "tram", "base_fare": 45.0},
        {"route_id": 3, "route_number": "M-1", "vehicle_type": "metro", "base_fare": 60.0},
    ])

# ---------- VEHICLES ----------
def gen_vehicles():
    return pd.DataFrame([
        {"vehicle_id": i, "route_id": random.randint(1, 3), "license_plate": f"A{i}BC77", "capacity": random.randint(50, 120)}
        for i in range(100, 120)
    ])

# ---------- RIDES ----------
def gen_rides(users, vehicles, n=5000):
    rows = []
    for _ in range(n):
        start = fake.date_time_this_month()
        end = start + timedelta(minutes=random.randint(5, 90))
        vehicle = vehicles.sample(1).iloc[0]
        rows.append({
            "ride_id": str(uuid.uuid4()),
            "user_id": users.sample(1).iloc[0].user_id,
            "route_id": vehicle.route_id,
            "vehicle_id": vehicle.vehicle_id,
            "start_time": start,
            "end_time": end,
            "fare_amount": random.choice([45, 50, 60])
        })
    return pd.DataFrame(rows)

# ---------- PAYMENTS ----------
def gen_payments(rides):
    return pd.DataFrame([
        {
            "payment_id": str(uuid.uuid4()),
            "ride_id": r.ride_id,
            "user_id": r.user_id,
            "amount": r.fare_amount,
            "payment_method": random.choice(["card", "sbp", "wallet"]),
            "status": random.choice(["success", "failed", "pending"]),
            "created_at": r.end_time
        }
        for _, r in rides.iterrows()
    ])

# ---------- SAVE ----------
def save(df, path):
    df.to_parquet(path, engine="pyarrow", storage_options=S3_OPTS)

if __name__ == "__main__":
    users = gen_users()
    routes = gen_routes()
    vehicles = gen_vehicles()
    rides = gen_rides(users, vehicles)
    payments = gen_payments(rides)

    save(users, "s3://raw/users/")
    save(routes, "s3://raw/routes/")
    save(vehicles, "s3://raw/vehicles/")
    save(rides, "s3://raw/rides/")
    save(payments, "s3://raw/payments/")


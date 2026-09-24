# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
import requests
import os
import time

# 1. Define the persistent path allowed by Databricks Serverless
bronze_path = "/Workspace/Shared/bronze"
os.makedirs(bronze_path, exist_ok=True)

year = "2026"
taxi_types = ["yellow", "green", "fhv", "fhvhv"]

# 2. Group months into quarters (Q1, Q2, Q3, Q4)
quarters = [
    ["01", "02", "03"],  # Q1
    ["04", "05", "06"],  # Q2
    ["07", "08", "09"],  # Q3
    ["10", "11", "12"]   # Q4
]

wait_time_seconds = 5 # Safety pause between quarters

print(f"Starting Bronze Massive Ingestion - Year {year}")
print("-" * 50)

for i, quarter in enumerate(quarters):
    print(f"\n🚀 Starting download for Quarter {i+1}...")
    
    for month in quarter:
        for taxi_type in taxi_types:
            # Build the dynamic URL
            source_name = f"{taxi_type}_tripdata_{year}-{month}.parquet"
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{source_name}"
            
            # Build the standardized destination path
            destination_file = f"{bronze_path}/{taxi_type}_tripdata_{year}_{month}.parquet"
            
            print(f" -> Fetching {taxi_type.upper()} for {year}-{month}...", end=" ")
            
            # Execute streaming download to protect RAM
            response = requests.get(url, stream=True)
            
            if response.status_code == 200:
                with open(destination_file, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        file.write(chunk)
                print("✅ Saved.")
            else:
                # Error handling if TLC hasn't published the month yet
                print(f"❌ Not available on TLC website (Not yet published).")
    
    # Apply timer if it's not the last quarter
    if i < len(quarters) - 1:
        print(f"\n⏳ Quarter {i+1} completed. Pausing for {wait_time_seconds}s to free up network load...")
        time.sleep(wait_time_seconds)

print("\n🎉 Bronze 2026 ingestion pipeline completed.")
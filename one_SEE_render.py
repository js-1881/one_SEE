import requests 
import pandas as pd
import numpy as np
import json
import os
import re
from thefuzz import fuzz, process
import time
from io import StringIO
import psutil
import gc
import openpyxl
import pytz
from openpyxl import load_workbook

from fastapi import FastAPI, File, UploadFile
from fastapi.responses import StreamingResponse, JSONResponse

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import threading

scheduler = None
app = FastAPI()


@app.get("/")
async def root():
    return {
        "status": "ok",
        "message": "n8n-Python service is running (SEE fetcher). Send your message to /process"
    }

def check_memory_usage():
    process = psutil.Process(os.getpid()) 
    memory_info = process.memory_info()  
    return memory_info.rss / 1024 ** 2  

def ram_check():
    print("memory usage:", check_memory_usage(), "MB")

def convert_date_or_keep_string(date):
    try:
        # Try to convert to datetime, assuming day-first format
        date_obj = pd.to_datetime(date, dayfirst=True, errors='raise')
        # If successful, format as 'yyyy-mm-dd'
        return date_obj.strftime('%Y-%m-%d')
    except ValueError:
        # If it's not a valid date, return it as is (non-date string)
        return date
    
def is_number(val):
    try:
        float(val)
        return True
    except (ValueError, TypeError):
        return False


@app.get("/health", include_in_schema=False)
def health():
    return {"ok": True}

# Health check ping function for self-pinging
def ping_self():
    try:
        # Get the Render external URL from environment variable, or use localhost for development
        base_url = os.getenv('RENDER_EXTERNAL_URL', 'https://one-see.onrender.com')
        
        print(f"🔄 Pinging {base_url}/health at {time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        response = requests.get(f"{base_url}/health", timeout=30)
        if response.status_code == 200:
            print(f"✅ Self-ping successful - Status: {response.status_code} at {time.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            print(f"❌ Self-ping failed with status {response.status_code} at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception as e:
        print(f"❌ Self-ping error: {e} at {time.strftime('%Y-%m-%d %H:%M:%S')}")

# Function to start the scheduler
def start_scheduler():
    global scheduler
    if scheduler and scheduler.running:
        print("✅ Scheduler is already running")
        return
    
    scheduler = BackgroundScheduler(daemon=True)
    
    # Add the self-pinging job to run every 2 minutes
    scheduler.add_job(
        ping_self,
        trigger=IntervalTrigger(minutes=11),
        id='self_ping',
        name='Self Ping Job',
        replace_existing=True
    )
    
    try:
        scheduler.start()
        print("🚀 Self-pinging scheduler STARTED - Will ping every 3 minutes")
        print("⏰ Next ping in 3 minutes...")
        
        # Do an immediate ping on startup
        print("🔄 Initial ping...")
        ping_self()
        
    except Exception as e:
        print(f"❌ Failed to start scheduler: {e}")

# Start the scheduler when the app starts
@app.on_event("startup")
async def startup_event():
    print("🔧 Starting up application...")
    
    # Start scheduler in a separate thread to avoid blocking
    scheduler_thread = threading.Thread(target=start_scheduler, daemon=True)
    scheduler_thread.start()
    
    # Give it a moment to start
    time.sleep(2)
    print("🏁 Application startup complete")

# Add a route to check scheduler status
@app.get("/scheduler-status")
async def scheduler_status():
    global scheduler
    if scheduler and scheduler.running:
        jobs = scheduler.get_jobs()
        return {
            "status": "running",
            "job_count": len(jobs),
            "next_ping": str(jobs[0].next_run_time) if jobs else "No jobs"
        }
    else:
        return {"status": "not running"}

# Add a route to manually trigger a ping
@app.get("/trigger-ping")
async def trigger_ping():
    ping_self()
    return {"message": "Manual ping triggered"}

valid_ids = []

# Endpoint to process the message text (from Slack)
@app.post("/process")
async def process_message(message: dict):
    # Extract text from the Slack message
    slack_text = message.get("text", "")

    # Initialize valid_ids list within the function
    valid_ids.clear()  # Clear the list to avoid accumulation from previous calls

    # Extract valid "SEE" IDs from the Slack message
    for word in slack_text.split():
        # Check if the word starts with 'SEE' (case-insensitive)
        if word.strip().lower().startswith("see"):
            valid_ids.append(word.strip().upper())  # Add to valid_ids (in uppercase)

    # Get unique 'SEE' IDs and count them
    unique_see = list(set(valid_ids))  # Remove duplicates by converting to set, then back to list
    count = len(unique_see)

    # Perform a RAM check (optional)
    ram_check()


    # Step 1: Get access token
    headers = {
        'accept': 'text/plain',
        'Content-Type': 'application/json',
    }


    json_data = {
        'email': 'lfritsch@flex-power.energy',
        'password': 'Ceciistlieb123.',
    }

    response = requests.post('https://api.blindleister.de/api/v1/authentication/get-access-token', headers=headers, json=json_data)

    if response.status_code != 200:
        print("Failed to get access token:", response.status_code, response.text)
        exit()

    token = response.text.strip('"')  # Remove potential extra quotes
    print("Access token:", token)


    # Step 2: Use the token to query market price
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJvcHNAZmxleC1wb3dlci5lbmVyZ3kifQ.Q1cDDds4fzzYFbW59UuZ4362FnmvBUQ8FY4UNhWp2a0'
    }


    # blindleister price
    # Fetch blindleister price
    print("🍚🍚")
    # === Years to fetch ===
    years = [2021, 2023, 2024]
    records = []

    # === Loop through each ID and fetch data for each year ===
    for site_id in valid_ids:
        print(f"Processing: {site_id}")

        for year in years:
            payload = {
                'ids': [site_id],
                'year': year
            }

            response = requests.post(
                'https://api.blindleister.de/api/v1/market-price-atlas-api/get-market-price', # market price atlas blindleister API
                headers = headers,
                json=payload
            )

            if response.status_code != 200:
                print(f"  Year {year}: Failed ({response.status_code}) - {response.text}")
                continue

            try:
                result = response.json()
                for entry in result:
                    entry['year'] = year
                    records.append(entry)
            except Exception as e:
                print(f"  Year {year}: Error parsing response - {e}")
                continue

    df_flat = pd.DataFrame(records)
    df_flat = pd.json_normalize(
        records,
        record_path="months",
        meta=[
            "year",
            "unit_mastr_id",
            "gross_power_kw",
            "energy_source",
            "annual_generated_energy_mwh",
            "benchmark_market_price_eur_mwh",
        ],
        errors="ignore"  # in case some records lack "months"
    )

    cols = [
        "year",
        "unit_mastr_id",
        "gross_power_kw",
        "energy_source",
        "annual_generated_energy_mwh",
        "benchmark_market_price_eur_mwh",
        "month",
        "monthly_generated_energy_mwh",
        "monthly_energy_contribution_percent",
        "monthly_market_price_eur_mwh",
        "monthly_reference_market_price_eur_mwh",
    ]

    df_flat = df_flat[cols]
    df_all_flat = df_flat.copy()

    print("🥨🥨🥨🥨🥨🥨")
    
    del df_flat
    gc.collect()

    df_all_flat['weighted_per_mwh_monthly'] = (
        ((df_all_flat['monthly_generated_energy_mwh'] * df_all_flat['monthly_market_price_eur_mwh']) -
            (df_all_flat['monthly_generated_energy_mwh'] * df_all_flat['monthly_reference_market_price_eur_mwh'])) /
        df_all_flat['monthly_generated_energy_mwh'] *
        df_all_flat['monthly_energy_contribution_percent'] / 100 * 12
    )

    df_all_flat['spot_rmv_EUR_ytd'] = (
    ((df_all_flat['monthly_generated_energy_mwh'] * df_all_flat['monthly_market_price_eur_mwh']) -
        (df_all_flat['monthly_generated_energy_mwh'] * df_all_flat['monthly_reference_market_price_eur_mwh']))
    )
    
    permalo_blind = df_all_flat.groupby(['unit_mastr_id'], dropna=False).agg(
                        spot_rmv_EUR_ytd = ('spot_rmv_EUR_ytd','sum'),
                        sum_prod_ytd = ('monthly_generated_energy_mwh','sum')
                    ).reset_index()

    permalo_blind['average_weighted_eur_mwh_blindleister'] = permalo_blind['spot_rmv_EUR_ytd'] / permalo_blind['sum_prod_ytd']
    
    print("🥨🥨🥨")
    ram_check()

    year_agg_per_unit = df_all_flat.groupby(['year', 'unit_mastr_id'])['weighted_per_mwh_monthly'].mean().reset_index(name='weighted_year_agg_per_unit_eur_mwh')


    df_year_agg_per_unit = pd.DataFrame(year_agg_per_unit)

    weighted_years_pivot = df_year_agg_per_unit.pivot(
        index='unit_mastr_id',
        columns='year',
        values='weighted_year_agg_per_unit_eur_mwh'
    ).reset_index()

    del df_year_agg_per_unit, year_agg_per_unit
    gc.collect()


    # Rename columns for clarity
    weighted_years_pivot.columns.name = None  # remove the axis name
    weighted_years_pivot = weighted_years_pivot.rename(columns={
        2021: 'weighted_2021_eur_mwh_blindleister',
        2023: 'weighted_2023_eur_mwh_blindleister',
        2024: 'weighted_2024_eur_mwh_blindleister'
    })

    # Add a column to average the available yearly weighted values
    weighted_years_pivot['average_weighted_eur_mwh_blindleister'] = weighted_years_pivot[
        ['weighted_2021_eur_mwh_blindleister', 'weighted_2023_eur_mwh_blindleister', 'weighted_2024_eur_mwh_blindleister']
    ].mean(axis=1, skipna=True)

    # Show only the desired columns
    final_weighted_blindleister = weighted_years_pivot[[
        'unit_mastr_id',
        'weighted_2021_eur_mwh_blindleister',
        'weighted_2023_eur_mwh_blindleister',
        'weighted_2024_eur_mwh_blindleister',
        #'average_weighted_eur_mwh_blindleister_avg'
    ]]
    
    
    final_weighted_blindleister = pd.merge(
        final_weighted_blindleister, 
        permalo_blind[["unit_mastr_id", "average_weighted_eur_mwh_blindleister"]],
        on= 'unit_mastr_id',
        how='left'
    )
    
    print(final_weighted_blindleister)

    data = final_weighted_blindleister.to_dict(orient='records')
    return {"data": data}















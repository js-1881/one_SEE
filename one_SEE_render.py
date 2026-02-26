import requests
import pandas as pd
import numpy as np
import os
import gc
import time
import psutil
import threading

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

# =============================================================================
# CONFIGURATION – read from Render environment variables
# =============================================================================

BLINDLEISTER_EMAIL = os.getenv("BLINDLEISTER_EMAIL", "").strip()
BLINDLEISTER_PASSWORD = os.getenv("BLINDLEISTER_PASSWORD", "").strip()
BLINDLEISTER_HARDCODED_TOKEN = os.getenv("BLINDLEISTER_HARDCODED_TOKEN", "").strip()

_BASE_URL = "https://api.blindleister.de"
_AUTH_URL = f"{_BASE_URL}/api/v1/authentication/get-access-token"
_MARKET_PRICE_URL = f"{_BASE_URL}/api/v1/market-price-atlas-api/get-market-price"

YEARS = [2021, 2023, 2024, 2025]

scheduler = None
app = FastAPI()

# Module-level token cache for freshly fetched tokens
_fresh_token: str | None = None


# =============================================================================
# HELPERS
# =============================================================================


def check_memory_usage():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024**2


def ram_check():
    print("memory usage:", check_memory_usage(), "MB")


# =============================================================================
# BLINDLEISTER AUTH  (mirrors upgrid_pricing_template.py pattern)
# =============================================================================


def _fetch_fresh_token() -> str:
    """Fetch a new Bearer token via email/password login."""
    global _fresh_token
    resp = requests.post(
        _AUTH_URL,
        headers={"accept": "text/plain", "Content-Type": "application/json"},
        json={"email": BLINDLEISTER_EMAIL, "password": BLINDLEISTER_PASSWORD},
    )
    if resp.status_code != 200:
        raise RuntimeError(
            f"Failed to get Blindleister token: {resp.status_code} - {resp.text}"
        )
    _fresh_token = resp.text.strip('"')
    print("✅ Fresh Blindleister token obtained")
    return _fresh_token


def get_blindleister_token(force_refresh: bool = False) -> str:
    """
    Return an auth token:
    1. Hardcoded token (from env) if set and not forcing refresh
    2. Cached fresh token if available
    3. Freshly fetched token
    """
    global _fresh_token
    if BLINDLEISTER_HARDCODED_TOKEN and not force_refresh:
        return BLINDLEISTER_HARDCODED_TOKEN
    if _fresh_token and not force_refresh:
        return _fresh_token
    return _fetch_fresh_token()


# =============================================================================
# BLINDLEISTER DATA FETCH
# =============================================================================


def fetch_blindleister_records(site_ids: list, years: list = YEARS) -> list:
    """
    Fetch raw market-price records from Blindleister for each site × year.
    Handles 401 by automatically refreshing the token once.
    """
    auth_token = get_blindleister_token()
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}",
    }

    records = []
    token_refreshed = False

    for site_id in site_ids:
        print(f"Processing: {site_id}")

        for year in years:
            payload = {"ids": [site_id], "year": year}
            resp = requests.post(_MARKET_PRICE_URL, headers=headers, json=payload)

            # Refresh token once on 401
            if resp.status_code == 401 and not token_refreshed:
                print("⚠️ Token expired/invalid – fetching fresh token and retrying...")
                auth_token = get_blindleister_token(force_refresh=True)
                headers["Authorization"] = f"Bearer {auth_token}"
                token_refreshed = True
                resp = requests.post(_MARKET_PRICE_URL, headers=headers, json=payload)

            if resp.status_code != 200:
                print(f"  Year {year}: Failed ({resp.status_code}) - {resp.text}")
                continue

            try:
                result = resp.json()
                for entry in result:
                    entry["year"] = year
                    records.append(entry)
            except Exception as e:
                print(f"  Year {year}: Error parsing response - {e}")

    return records


# =============================================================================
# DATA PROCESSING
# =============================================================================


def process_blindleister_records(records: list) -> pd.DataFrame:
    """
    Transform raw Blindleister API records into a summary DataFrame.

    Output columns per unit_mastr_id:
    - weighted_2021/2023/2024/2025_eur_mwh_blindleister  (yearly weighted RMV delta)
    - average_weighted_eur_mwh_blindleister               (production-weighted overall)
    - energy_source
    - adjusted_pv, adjusted_pv_low, adjusted_pv_high      (Adjusted_PV = -1.9 + 1.3*blind ± 1.6)
    - adjusted_wind, adjusted_wind_low, adjusted_wind_high (Adjusted_WIND = -2 + 1.1*blind ± 2.7)
    """
    if not records:
        return pd.DataFrame()

    df = pd.json_normalize(
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
        errors="ignore",
    )

    keep_cols = [
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
    df = df[[c for c in keep_cols if c in df.columns]].copy()

    print("🥨🥨🥨🥨🥨🥨")

    # Monthly spot-RMV delta (EUR)
    df["spot_rmv_EUR_monthly"] = (
        df["monthly_generated_energy_mwh"] * df["monthly_market_price_eur_mwh"]
        - df["monthly_generated_energy_mwh"]
        * df["monthly_reference_market_price_eur_mwh"]
    )

    # Monthly weighted value per MWh (annualised, contribution-weighted)
    df["weighted_per_mwh_monthly"] = (
        df["spot_rmv_EUR_monthly"]
        / df["monthly_generated_energy_mwh"]
        * df["monthly_energy_contribution_percent"]
        / 100
        * 12
    )

    # --- Overall production-weighted average across all years ---
    permalo_blind = (
        df.groupby("unit_mastr_id", dropna=False)
        .agg(
            spot_rmv_EUR_ytd=("spot_rmv_EUR_monthly", "sum"),
            sum_prod_ytd=("monthly_generated_energy_mwh", "sum"),
        )
        .reset_index()
    )
    permalo_blind["average_weighted_eur_mwh_blindleister"] = (
        permalo_blind["spot_rmv_EUR_ytd"] / permalo_blind["sum_prod_ytd"]
    )

    print("🥨🥨🥨")
    ram_check()

    # --- Yearly weighted values (mean of monthly contribution-weighted values) ---
    year_agg = (
        df.groupby(["year", "unit_mastr_id"])["weighted_per_mwh_monthly"]
        .mean()
        .reset_index(name="weighted_year_agg_per_unit_eur_mwh")
    )

    weighted_years_pivot = year_agg.pivot(
        index="unit_mastr_id",
        columns="year",
        values="weighted_year_agg_per_unit_eur_mwh",
    ).reset_index()

    del year_agg
    gc.collect()

    weighted_years_pivot.columns.name = None
    weighted_years_pivot = weighted_years_pivot.rename(
        columns={
            2021: "weighted_2021_eur_mwh_blindleister",
            2023: "weighted_2023_eur_mwh_blindleister",
            2024: "weighted_2024_eur_mwh_blindleister",
            2025: "weighted_2025_eur_mwh_blindleister",
        }
    )

    # --- Build final table ---
    year_cols = [
        "weighted_2021_eur_mwh_blindleister",
        "weighted_2023_eur_mwh_blindleister",
        "weighted_2024_eur_mwh_blindleister",
        "weighted_2025_eur_mwh_blindleister",
    ]
    pivot_cols = ["unit_mastr_id"] + [
        c for c in year_cols if c in weighted_years_pivot.columns
    ]
    final = weighted_years_pivot[pivot_cols].copy()

    # Merge overall average (production-weighted)
    final = pd.merge(
        final,
        permalo_blind[["unit_mastr_id", "average_weighted_eur_mwh_blindleister"]],
        on="unit_mastr_id",
        how="left",
    )

    # Merge energy_source (first observed value per unit)
    energy_map = df.groupby("unit_mastr_id")["energy_source"].first().reset_index()
    final = pd.merge(final, energy_map, on="unit_mastr_id", how="left")

    # Round EUR/MWh columns
    eur_cols = [c for c in final.columns if "eur_mwh" in str(c).lower()]
    final[eur_cols] = final[eur_cols].round(2)

    # --- Adjusted price columns ---
    blind = final["average_weighted_eur_mwh_blindleister"]

    # Adjusted_PV = -1.9 + 1.3 * blind ± 1.6
    final["adjusted_pv"] = (-1.9 + 1.3 * blind).round(2)
    final["adjusted_pv_low"] = (final["adjusted_pv"] - 1.6).round(2)
    final["adjusted_pv_high"] = (final["adjusted_pv"] + 1.6).round(2)

    # Adjusted_WIND = -2 + 1.1 * blind ± 2.7
    final["adjusted_wind"] = (-2 + 1.1 * blind).round(2)
    final["adjusted_wind_low"] = (final["adjusted_wind"] - 2.7).round(2)
    final["adjusted_wind_high"] = (final["adjusted_wind"] + 2.7).round(2)

    # Unified adjusted column based on energy_source
    src = final["energy_source"].astype(str)
    is_pv = src.str.contains(r"PV|solar", case=False, regex=True, na=False)
    is_wind = src.str.contains(r"wind", case=False, regex=True, na=False)

    final["adjusted"] = np.where(
        is_pv, final["adjusted_pv"], np.where(is_wind, final["adjusted_wind"], np.nan)
    )
    final["adjusted_low"] = np.where(
        is_pv,
        final["adjusted_pv_low"],
        np.where(is_wind, final["adjusted_wind_low"], np.nan),
    )
    final["adjusted_high"] = np.where(
        is_pv,
        final["adjusted_pv_high"],
        np.where(is_wind, final["adjusted_wind_high"], np.nan),
    )

    return final


# =============================================================================
# FASTAPI ROUTES
# =============================================================================


@app.get("/")
async def root():
    return {
        "status": "ok",
        "message": "n8n-Python service is running (SEE fetcher). Send your message to /process",
    }


@app.get("/health", include_in_schema=False)
def health():
    return {"ok": True}


# =============================================================================
# SELF-PING SCHEDULER  (keeps Render free tier alive)
# =============================================================================


def ping_self():
    try:
        base_url = os.getenv("RENDER_EXTERNAL_URL", "https://one-see.onrender.com")
        print(f"🔄 Pinging {base_url}/health at {time.strftime('%Y-%m-%d %H:%M:%S')}")
        response = requests.get(f"{base_url}/health", timeout=30)
        if response.status_code == 200:
            print(
                f"✅ Self-ping successful - Status: {response.status_code} at {time.strftime('%Y-%m-%d %H:%M:%S')}"
            )
        else:
            print(
                f"❌ Self-ping failed with status {response.status_code} at {time.strftime('%Y-%m-%d %H:%M:%S')}"
            )
    except Exception as e:
        print(f"❌ Self-ping error: {e} at {time.strftime('%Y-%m-%d %H:%M:%S')}")


def start_scheduler():
    global scheduler
    if scheduler and scheduler.running:
        print("✅ Scheduler is already running")
        return

    scheduler = BackgroundScheduler(daemon=True)
    scheduler.add_job(
        ping_self,
        trigger=IntervalTrigger(minutes=8),
        id="self_ping",
        name="Self Ping Job",
        replace_existing=True,
    )

    try:
        scheduler.start()
        print("🚀 Self-pinging scheduler STARTED - Will ping every 8 minutes")
        print("⏰ Next ping in 8 minutes...")
        print("🔄 Initial ping...")
        ping_self()
    except Exception as e:
        print(f"❌ Failed to start scheduler: {e}")


@app.on_event("startup")
async def startup_event():
    print("🔧 Starting up application...")
    scheduler_thread = threading.Thread(target=start_scheduler, daemon=True)
    scheduler_thread.start()
    time.sleep(2)
    print("🏁 Application startup complete")


@app.get("/scheduler-status")
async def scheduler_status():
    global scheduler
    if scheduler and scheduler.running:
        jobs = scheduler.get_jobs()
        return {
            "status": "running",
            "job_count": len(jobs),
            "next_ping": str(jobs[0].next_run_time) if jobs else "No jobs",
        }
    return {"status": "not running"}


@app.get("/trigger-ping")
async def trigger_ping():
    ping_self()
    return {"message": "Manual ping triggered"}


# =============================================================================
# MAIN PROCESS ENDPOINT
# =============================================================================


@app.post("/process")
async def process_message(message: dict):
    slack_text = message.get("text", "")

    # Extract SEE IDs from Slack message (local variable – no shared state)
    valid_ids = []
    for word in slack_text.split():
        if word.strip().lower().startswith("see"):
            valid_ids.append(word.strip().upper())

    unique_see = list(set(valid_ids))
    print(f"🦐 SEE IDs to process: {unique_see}")
    ram_check()

    records = fetch_blindleister_records(unique_see)

    if not records:
        print("❌ No records fetched from Blindleister")
        return JSONResponse(
            content={"data": [], "error": "No data fetched from Blindleister"},
            status_code=200,
        )

    final = process_blindleister_records(records)
    gc.collect()

    print(final)
    ram_check()

    return {"data": final.to_dict(orient="records")}

import os
import time
from datetime import date, timedelta
import requests
import pandas as pd
from dotenv import load_dotenv


def configure():
    load_dotenv()
    api_key = os.getenv("OPENWEATHER_API_KEY")
    if not api_key:
        raise ValueError("Missing OPENWEATHER_API_KEY")
    return api_key


REGIONS = {
    "US-FLA-FPL": {"name": "Tampa", "lat": 27.95, "lon": -82.46},
    "US-CAL-CISO": {"name": "Los Angeles", "lat": 34.05, "lon": -118.24},
    "US-NY-NYIS": {"name": "New York City", "lat": 40.71, "lon": -74.01},
}

OUTPUT_PARQUET = "weather_daily.parquet"


def generate_date_range(start_date: date, end_date: date):
    d = start_date
    while d <= end_date:
        yield d
        d += timedelta(days=1)


def fetch_daily_summary(api_key: str, lat: float, lon: float, day: date, session=None):
    """Fetch one day of weather data (daily aggregation)."""
    if session is None:
        session = requests.Session()

    url = "https://api.openweathermap.org/data/3.0/onecall/day_summary"
    params = {
        "lat": lat,
        "lon": lon,
        "date": day.strftime("%Y-%m-%d"),
        "units": "metric",
        "appid": api_key,
    }

    r = session.get(url, params=params)
    if r.status_code == 200:
        return r.json()

    print(f"[WARN] {day} status={r.status_code} msg={r.text[:120]}")
    return None


def parse_daily_record(region_id: str, meta: dict, day: date, data: dict):
    temperature = data.get("temperature", {})  # from day_summary
    t_min = temperature.get("min")
    t_max = temperature.get("max")

    # Use afternoon as "mean" if existir, senão média simples min/max
    t_mean = temperature.get("afternoon")
    if t_mean is None and t_min is not None and t_max is not None:
        t_mean = (t_min + t_max) / 2

    return {
        "date": day,
        "region": region_id,
        "city_name": meta["name"],
        "lat": data.get("lat", meta["lat"]),
        "lon": data.get("lon", meta["lon"]),
        "temp_min": t_min,
        "temp_max": t_max,
        "temp_mean": t_mean,
    }


def fetch_weather_range(
    start_date: date,
    end_date: date,
    regions: dict,
    max_calls: int = 900,
    sleep_seconds: float = 0.0,
):
    """Fetch weather for multiple regions across a date range."""
    api_key = configure()
    session = requests.Session()
    rows = []
    calls = 0

    for d in generate_date_range(start_date, end_date):
        for region_id, meta in regions.items():
            if calls >= max_calls:
                print(f"[INFO] Max calls reached: {calls}")
                return rows

            data = fetch_daily_summary(api_key, meta["lat"], meta["lon"], d, session)
            calls += 1

            if data is not None:
                rows.append(parse_daily_record(region_id, meta, d, data))

            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

    print(f"[INFO] Done. Calls used: {calls}")
    return rows


def append_to_parquet(rows, output_path: str):
    """Append new rows to Parquet (deduplicate by date+region)."""
    if not rows:
        print("[INFO] No rows to save.")
        return

    new_df = pd.DataFrame(rows)

    if os.path.exists(output_path):
        old_df = pd.read_parquet(output_path)
        df = pd.concat([old_df, new_df], ignore_index=True)
        df = df.drop_duplicates(subset=["date", "region"])
        df.to_parquet(output_path, index=False)
    else:
        new_df.to_parquet(output_path, index=False)

    print(f"[INFO] Saved {len(new_df)} new rows.")


def main():
    start = date(2023, 1, 1)
    end = date(2023, 12, 31)

    test_regions = {
        "US-FLA-FPL": REGIONS["US-FLA-FPL"],
    }

    rows = fetch_weather_range(
        start_date=start,
        end_date=end,
        regions=test_regions,
        max_calls=366,
    )

    append_to_parquet(rows, OUTPUT_PARQUET)


if __name__ == "__main__":
    main()

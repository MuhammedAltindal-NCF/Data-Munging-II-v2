import os
from datetime import date, timedelta

import requests
import pandas as pd
from dotenv import load_dotenv

# List of cities we want to query:
CITIES = ["Los Angeles", "New York", "Tampa"]

def configure():
    load_dotenv()

def get_weather_for_day(session, city_name, day):
    """
    Weather API call for a specific city and day.
    Adjust this URL according to the endpoint you are actually using.
    Right now this is using a standard OpenWeather endpoint as an example.
    If you are using the "historical" endpoint, you may need to add parameters like &dt=...
    """
    api_key = os.getenv("weather_api_key")
    if not api_key:
        raise RuntimeError("weather_api_key not found in the .env file.")

    # TODO: If using a historical endpoint, modify according to your URL requirements.
    # For now, this is based on the URL shown in your screenshot:
    url = (
        f"https://api.openweathermap.org/data/2.5/weather"
        f"?q={city_name}&units=imperial&appid={api_key}"
    )

    r = session.get(url)
    r.raise_for_status()
    return r.json()

def main():
    configure()
    s = requests.Session()

    # ==============================
    # 1) UPDATE THESE DATES MANUALLY WHENEVER YOU NEED
    #
    # Daily quota calculation (for 3 cities):
    # 1000 total requests - 100 safety buffer = 900 usable requests
    # 900 / 3 cities = 300 safe days
    #
    # EXAMPLE: Choose a range such as 2021-01-01 to 2021-10-28 (around 300 days).
    #
    # IMPORTANT: Keep the range below 300 days unless you reduce the number of cities.
    # ==============================
    start_date = date(2021, 1, 1)   # START DATE
    end_date   = date(2021, 3, 31)  # END DATE (keep within ~300 days)
    # ==============================

    rows = []
    current = start_date

    while current <= end_date:
        for city in CITIES:
            data = get_weather_for_day(s, city, current)

            rows.append({
                "city": city,
                "date": current.isoformat(),
                "temp": data["main"]["temp"],
                "feels_like": data["main"]["feels_like"],
                "humidity": data["main"]["humidity"],
                "wind_speed": data["wind"]["speed"],
                "timestamp": data.get("dt"),  # UNIX timestamp returned by API
            })

        current += timedelta(days=1)

    df = pd.DataFrame(rows)

    # Save file with date range included in the name:
    filename = f"weather_{start_date}_{end_date}.csv"
    df.to_csv(filename, index=False)

    print(f"Saved → {filename}")
    print(f"Total API calls: {len(rows)}")

if __name__ == "__main__":
    main()

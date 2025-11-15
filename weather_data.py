import requests
from dotenv import load_dotenv
import os
import pandas as pd


def configure():
    load_dotenv()

def get_current_weather(session, city_name):
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city_name}&units=imperial&appid={os.getenv('weather_api_key')}"
    r = session.get(url)
    return r.json()

def main():
    configure()
    s = requests.Session()
    losangeles = get_current_weather(s, 'Los Angeles')
    newy = get_current_weather(s, 'New York')
    tampa = get_current_weather(s, 'Tampa')
    print(losangeles, newy, tampa)

def main():
    configure()
    s = requests.Session()

    losangeles = get_current_weather(s, 'Los Angeles')
    newy = get_current_weather(s, 'New York')
    tampa = get_current_weather(s, 'Tampa')

    rows = []
    for data in [losangeles, newy, tampa]:
        rows.append({
            "city": data["name"],
            "temp": data["main"]["temp"],
            "feels_like": data["main"]["feels_like"],
            "humidity": data["main"]["humidity"],
            "wind_speed": data["wind"]["speed"],
            "timestamp": data["dt"]
        })

    df = pd.DataFrame(rows)
    print(df)

main()
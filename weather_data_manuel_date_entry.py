import os
from datetime import date, timedelta

import requests
import pandas as pd
from dotenv import load_dotenv

# 3 şehrimiz:
CITIES = ["Los Angeles", "New York", "Tampa"]

def configure():
    load_dotenv()

def get_weather_for_day(session, city_name, day):
    """
    Belirli bir gün ve şehir için hava durumu çağrısı.
    Buradaki URL'yi senin kullandığın endpoint'e göre ayarla.
    Şu an örnek olsun diye OpenWeather URL'si var.
    """
    api_key = os.getenv("weather_api_key")
    if not api_key:
        raise RuntimeError("weather_api_key .env dosyasında bulunamadı.")

    # TODO: Eğer history endpoint kullanıyorsan &dt=... vs eklemen gerekebilir.
    # Şimdilik senin ekran görüntündeki URL'yi baz alıyorum:
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
    # 1) BURAYI HER SEFERİNDE MANUEL DEĞİŞTİRECEKSİN
    # Günlük kota hesabı (3 şehir için):
    # 1000 toplam - 100 güvenlik = 900 çağrı
    # 900 / 3 = 300 güne kadar güvenli
    #
    # ÖRNEK: 01-01-2021 ile 28-10-2021 arası 300 gün gibi bir aralık seç.
    start_date = date(2021, 1, 1)   # BAŞLANGIÇ TARİHİ
    end_date   = date(2021, 3, 31)  # BİTİŞ TARİHİ (en fazla 300 gün olsun)
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
                "timestamp": data.get("dt"),
            })

        current += timedelta(days=1)

    df = pd.DataFrame(rows)

    # Dosya adını da tarih aralığına göre verelim:
    filename = f"weather_{start_date}_{end_date}.csv"
    df.to_csv(filename, index=False)
    print(f"Kaydedildi → {filename}")
    print(f"Toplam API çağrısı: {len(rows)}")

if __name__ == "__main__":
    main()

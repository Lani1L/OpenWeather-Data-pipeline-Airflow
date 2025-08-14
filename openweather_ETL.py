import pandas as pd
import requests
from datetime import datetime, timedelta
import time
import logging

def run_etl(cities_file="cities.txt", cred_file="credential.txt", output_file="weather_data.csv"):
    """Fetch weather data for a list of cities and save to CSV."""
    logging.basicConfig(level=logging.INFO)

    def kelvin_to_celsius(kelvin):
        return kelvin - 273.15

    try:
        with open(cities_file, "r") as f:
            cities = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        logging.error(f"{cities_file} not found.")
        return

    try:
        with open(cred_file, "r") as f:
            api_key = f.read().strip()
    except FileNotFoundError:
        logging.error(f"{cred_file} not found.")
        return

    if not api_key:
        logging.error("API key is empty.")
        return

    base_url = "https://api.openweathermap.org/data/2.5/weather?q="
    all_weather_data = []

    for city in cities:
        full_url = f"{base_url}{city}&appid={api_key}"
        try:
            response = requests.get(full_url)
            weather = response.json()
            if response.status_code == 200 and "main" in weather:
                city = weather["name"]
                weather_description = weather["weather"][0]["description"]
                temp_celsius = round(kelvin_to_celsius(weather["main"]["temp"]), 2)
                feels_like_celsius = round(kelvin_to_celsius(weather["main"]["feels_like"]), 2)
                min_temp_celsius = round(kelvin_to_celsius(weather["main"]["temp_min"]), 2)
                max_temp_celsius = round(kelvin_to_celsius(weather["main"]["temp_max"]), 2)
                humidity = weather["main"]["humidity"]
                pressure = weather["main"]["pressure"]
                wind_speed = weather["wind"]["speed"]
                wind_direction = weather["wind"].get("deg", 0)
                time_of_record = datetime.utcfromtimestamp(weather["dt"]) + timedelta(hours=8)
                sunrise_time = datetime.utcfromtimestamp(weather["sys"]["sunrise"]) + timedelta(hours=8)
                sunset_time = datetime.utcfromtimestamp(weather["sys"]["sunset"]) + timedelta(hours=8)
                weather_data = {
                    "city": city,
                    "weather_description": weather_description,
                    "temperature_celsius": temp_celsius,
                    "feels_like_celsius": feels_like_celsius,
                    "min_temperature_celsius": min_temp_celsius,
                    "max_temperature_celsius": max_temp_celsius,
                    "humidity": humidity,
                    "pressure": pressure,
                    "wind_speed": wind_speed,
                    "wind_direction": wind_direction,
                    "time_of_record": time_of_record.strftime("%Y-%m-%d %H:%M"),
                    "sunrise_time": sunrise_time.strftime("%Y-%m-%d %H:%M"),
                    "sunset_time": sunset_time.strftime("%Y-%m-%d %H:%M")
                }
                all_weather_data.append(weather_data)
            else:
                logging.warning(f"Failed to get data for {city}: {weather.get('message', 'Unknown error')}")
        except requests.RequestException as e:
            logging.error(f"Request error for {city}: {e}")
        time.sleep(1)

    df_weather = pd.DataFrame(all_weather_data)
    if not df_weather.empty:
        df_weather.to_csv("s3//your_s3_bucket/openweather_data.csv", index=False)
        logging.info(f"Weather data saved to {output_file}")
    else:
        logging.warning("No weather data collected.")

if __name__ == "__main__":
    run_etl()

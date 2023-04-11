

# Imports
import os
import requests
import json
import sqlite3
from datetime import datetime, timedelta

# Task 1:

def create_database():
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+"final.database")
    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS weather_data (
                 date TEXT,
                 max_temp FLOAT,
                 min_temp FLOAT,
                 avg_temp FLOAT,
                 max_wind FLOAT,
                 precip FLOAT,
                 condition TEXT
                 )''')
    conn.commit()
    return cur,conn
    

def get_weather_api(key, location, start_date, end_date):
    url = f'https://api.weatherapi.com/v1/history.json?key={key}&q={location}&dt={start_date}&end_dt={end_date}'
    response = requests.get(url)
    data = response.text
    j_dict = json.loads(data)
    return j_dict

def weather_data_table(cur, conn, data):
    for day in data['forecast']['forecastday']:
        date = day['date']
        max_temp = day['day']['maxtemp_f']
        min_temp = day['day']['mintemp_f']
        avg_temp = day['day']['avgtemp_f']
        max_wind = day['day']['maxwind_mph']
        precip = day['day']['totalprecip_in']
        condition = day['day']['condition']['text']

        # Insert the data into the database table
        cur.execute("INSERT OR IGNORE INTO weather_data (date, max_temp, min_temp, avg_temp, max_wind, precip, condition) VALUES (?, ?, ?, ?, ?, ?, ?)", 
            (date, max_temp, min_temp, avg_temp, max_wind, precip, condition))
    conn.commit()



def main():
    # API key for accessing the WeatherAPI
    key = "339838be60e74f79a30210717230904"

    # Location and date range for the weather data
    location = 'New York'
    start_date = datetime(2022, 5, 1)
    end_date = datetime(2022, 8, 9)
    #add_days = timedelta(days=31)
   
    

    cur, conn = create_database()
    dates = []
    while len(dates) < 100:
        print('inside while loop')
        end_date = start_date + timedelta(days = 25)
        if end_date > datetime(2022, 8, 9):
            end_date = datetime(2022,8,9)
        if end_date not in dates:
            dates.append(end_date)
            print(f"Processing weather data for {start_date} to {end_date}")
            data = get_weather_api(key,location, start_date, end_date)
            weather_data_table(cur, conn,data)
        start_date = end_date + timedelta(days=1)
    conn.close()

if __name__ == "__main__":
    main()

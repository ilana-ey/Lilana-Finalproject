

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
    #cur.execute("DROP TABLE IF EXISTS weather_data")
    cur.execute('''CREATE TABLE IF NOT EXISTS weather_data (
                 date TEXT PRIMARY KEY,
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
    count = 0
    if data == []:
        return None
    for day in data['forecast']['forecastday']:
        date = day['date']
        max_temp = day['day']['maxtemp_f']
        min_temp = day['day']['mintemp_f']
        avg_temp = day['day']['avgtemp_f']
        max_wind = day['day']['maxwind_mph']
        precip = day['day']['totalprecip_in']
        condition = day['day']['condition']['text']


        # Check if a row already exists with the same date
        cur.execute("SELECT COUNT(*) FROM weather_data WHERE date=?", (date,))
        if cur.fetchone()[0] == 0:
            if count == 25:
                break
            count += 1
    
        # Insert the data into the database table
            cur.execute('''INSERT OR IGNORE INTO weather_data (
                date, max_temp, min_temp, avg_temp, max_wind, precip, condition) 
                VALUES (?, ?, ?, ?, ?, ?, ?)''', 
                (date, max_temp, min_temp, avg_temp, max_wind, precip, condition))

    conn.commit()
    



def main():
    # API key for accessing the WeatherAPI
    key = "339838be60e74f79a30210717230904"

    # Location and date range for the weather data
    location = 'New York'
    start_date = datetime(2022, 5, 1)
    end_date = datetime(2022, 8, 31)
    
    
    cur, conn = create_database()


    cur.execute("SELECT COUNT(*) FROM weather_data")
    rows = cur.fetchone()[0]    
    start_date = start_date + timedelta(days = rows)

    

    data = get_weather_api(key, location, start_date, end_date)
    if start_date > end_date:
        data = []
    weather_data_table(cur, conn, data)
    
    # api only allows for 35 days at a time so loop through
    cur.execute("SELECT COUNT(*) FROM weather_data")
    rows = cur.fetchone()[0]  
    print(rows)

    conn.close()



if __name__ == "__main__":
    main()

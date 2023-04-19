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
    cur.execute("CREATE TABLE IF NOT EXISTS dates (id INTEGER, date TEXT)")
    cur.execute('''CREATE TABLE IF NOT EXISTS weather_data (
                 id INTEGER PRIMARY KEY,
                 max_temp FLOAT,
                 min_temp FLOAT,
                 avg_temp FLOAT,
                 max_wind FLOAT,
                 precip FLOAT,
                 condition TEXT
                 )''')
    conn.commit()

    return cur,conn

def insert_dates_table(cur, conn, start_date,end_date):
    id = 1
    while start_date <= end_date:
        date_string = start_date.strftime("%Y-%m-%d")
        cur.execute("INSERT INTO dates (id, date) VALUES (?,?)", (id, date_string))
        start_date += timedelta(days=1)
        id += 1

    conn.commit()

def get_weather_api(key, location, start_date, end_date):
    url = f'https://api.weatherapi.com/v1/history.json?key={key}&q={location}&dt={start_date}&end_dt={end_date}'
    response = requests.get(url)
    data = response.text
    j_dict = json.loads(data)
    return j_dict
   



def weather_data_table(cur, conn, data, id):
    count = 0
    if data == []:
        return None
     
    for day in data['forecast']['forecastday']:
        date = day['date']
        
        # Check if id is in dates table
        cur.execute("SELECT id FROM dates WHERE date=?", (date,))
        row = cur.fetchone()
        if row is not None:
            id = row[0]
        else:
            continue

        # Check if a row already exists with the same date
        cur.execute("SELECT COUNT(*) FROM weather_data WHERE id=?", (id,))
        if cur.fetchone()[0] == 0:
            count += 1
        
      
        max_temp = day['day']['maxtemp_f']
        min_temp = day['day']['mintemp_f']
        avg_temp = day['day']['avgtemp_f']
        max_wind = day['day']['maxwind_mph']
        precip = day['day']['totalprecip_in']
        condition = day['day']['condition']['text']

        cur.execute('''INSERT OR IGNORE INTO weather_data (
                id, max_temp, min_temp, avg_temp, max_wind, precip, condition) 
                VALUES (?,?, ?, ?, ?, ?, ?)''', 
                (id, max_temp, min_temp, avg_temp, max_wind, precip, condition))
        
        if count % 25 == 0:
            break
                                       
    conn.commit()
    return data


#CALCULATIONS 

def calculate_average_temperatures(cur):
    cur.execute("SELECT strftime('%m', dates.date) as month, AVG(avg_temp) as avg_temp FROM weather_data JOIN dates ON weather_data.id = dates.id GROUP BY month")
    rows = cur.fetchall()
    full_path = os.path.join(os.path.dirname(__file__), 'averagetemp.txt')
    file = open(full_path,'w')
    file.write("The average temperature for each month from May 1, 2022 to August 31, 2022:\n")
    for row in rows:
        month = row[0]
        avg_temp = row[1]
        file.write(f'{month} is {avg_temp:.2f}°F.\n')
    file.close()

    
    

def calculate_most_common_condition_per_week(cur):
    cur.execute("""SELECT strftime('%W', dates.date) as week, 
                   weather_data.condition, COUNT(*) as count 
                   FROM weather_data 
                   JOIN dates ON weather_data.id = dates.id 
                   GROUP BY week, weather_data.condition 
                   ORDER BY week, count DESC""")
    rows = cur.fetchall()
    results = {}

    for row in rows:
        week = row[0]
        condition = row[1]
        count = row[2]
        
        if week not in results:
            results[week] = []
        
        results[week].append((condition, count))
    
    most_common_conditions = {}
    for week, conditions in results.items():
        most_common_conditions[week] = max(conditions, key=lambda x: x[1])[0]
    
    full_path = os.path.join(os.path.dirname(__file__), 'weekly_conditions.txt')
    file = open(full_path,'w')
    file.write("The conditions for each week from May 1, 2022 to August 31, 2022 with the first week as the 17th week of the year:\n")
    file.write(f'{most_common_conditions}')
    file.close()

    


def main():
    # API key for accessing the WeatherAPI
    key = "339838be60e74f79a30210717230904"

    # Location and date range for the weather data
    location = 'New York'
    start_date = datetime(2022, 5, 1)
    end_date = datetime(2022, 8, 31)
    
    
    cur, conn = create_database()

    cur.execute("SELECT COUNT(*) FROM dates")
    if cur.fetchone()[0] == 0:
        insert_dates_table(cur, conn, start_date, end_date)

       
    cur.execute("SELECT COUNT(*) FROM weather_data")
    rows = cur.fetchone()[0]    
    start_date = start_date + timedelta(days = rows)



    data = get_weather_api(key, location, start_date, end_date)
    if start_date > end_date:
        data = []
    weather_data_table(cur, conn, data, 1)

     # api only allows for 35 days at a time so loop through
    cur.execute("SELECT COUNT(*) FROM weather_data")
    rows = cur.fetchone()[0]
    print(rows)
  
    calculate_average_temperatures(cur)
    calculate_most_common_condition_per_week(cur)
    conn.close()



if __name__ == "__main__":
    main()
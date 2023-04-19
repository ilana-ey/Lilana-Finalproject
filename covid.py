import os
import requests
import sqlite3
import json
from datetime import datetime, timedelta
from bs4 import BeautifulSoup


def connect_database():
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+"final.database")
    cur = conn.cursor()
    
    cur.execute('''CREATE TABLE IF NOT EXISTS covid_data (
        id INTEGER PRIMARY KEY,
        test_positivity_ratio REAL,
        case_density REAL,
        weekly_new_cases REAL,
        infection_rate REAL,
        beds_with_covid_patients REAL,
        weekly_covid_admissions REAL
        )''')

    conn.commit()
    return cur, conn


def get_covid_data(api_key):
    url = f'https://api.covidactnow.org/v2/country/US.timeseries.json?apiKey={api_key}'


    response = requests.get(url)
    data = response.text
    j_dict = json.loads(data)
    covid_dict = {}
    for date in j_dict["metricsTimeseries"]:
        if date["date"][5:7] == "05" or date["date"][5:7] == "06" or date["date"][5:7] == "07" or date["date"][5:7] == "08":
            covid_dict[date["date"]] = date
    return covid_dict
    


def insert_covid_data(cur, conn, covid_dict, id):
    count = 0
    if covid_dict == []:
        return None
    
    for date, data in covid_dict.items():
        date = date
        # Check if id is in dates table
        cur.execute("SELECT id FROM dates WHERE date=?", (date,))
        row = cur.fetchone()
        if row is not None:
            id = row[0]
        else:
            continue

        # Check if a row already exists with the same date
        cur.execute("SELECT COUNT(*) FROM covid_data WHERE id=?", (id,))
        if cur.fetchone()[0] == 0:
            count += 1

            pos_ratio = data['testPositivityRatio']
            case_den = data['caseDensity']
            weekly_case = data['weeklyNewCasesPer100k']
            inf_rate = data['infectionRate']
            beds = data['bedsWithCovidPatientsRatio']
            weekly_ad = data['weeklyCovidAdmissionsPer100k']
            # Insert row
            cur.execute('''INSERT OR IGNORE INTO covid_data (
                    id, test_positivity_ratio, case_density, weekly_new_cases,
                    infection_rate,beds_with_covid_patients, weekly_covid_admissions) 
                    VALUES (?,?,?,?,?,?,?)''', 
                    (id, pos_ratio, case_den, weekly_case, inf_rate, beds, weekly_ad))
            
        if count == 25:
            break


    conn.commit()
 



def main():
    api_key = '556b11830164440ca5984cb2056669e7'
    start_date = datetime(2022, 5, 1)
    end_date = datetime(2022, 8, 31)

    cur, conn = connect_database()
       
    cur.execute("SELECT COUNT(*) FROM covid_data")
    rows = cur.fetchone()[0]    
    start_date = start_date + timedelta(days = rows)


    covid_dict = get_covid_data(api_key)
    if start_date > end_date:
        covid_dict = []
    insert_covid_data(cur, conn, covid_dict, 1)

     
    cur.execute("SELECT COUNT(*) FROM covid_data")
    rows = cur.fetchone()[0]
    print(rows)
  
    conn.close()

if __name__ == '__main__':
    main()
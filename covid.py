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
    print(covid_dict)
    return covid_dict


def insert_covid_data(cur, conn, covid_dict):
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
 

#CALCULATIONS#

def calculate_total_cases(cur):
    cur.execute("SELECT strftime('%m', dates.date) as month, SUM(covid_data.weekly_new_cases) as total_cases FROM covid_data JOIN dates ON covid_data.id=dates.id WHERE dates.date BETWEEN '2022-05-01' AND '2022-08-31' GROUP BY month")
    rows = cur.fetchall()
    full_path = os.path.join(os.path.dirname(__file__), 'totalcases.txt')
    file = open(full_path,'w')
    file.write("The total cases for each month from May 1, 2022 to August 31, 2022 Month:\n")
    for row in rows:
        file.write(f"{row[0]}, Total Cases: {row[1]}\n")
    file.close()

def calculate_avg_weekly_covid_admissions(cur):
    # Query the database for the average weekly covid admissions for each week
    cur.execute("""
        SELECT strftime('%W', dates.date) AS week, AVG(covid_data.weekly_covid_admissions) AS avg_weekly_covid_admissions
        FROM dates
        INNER JOIN covid_data ON dates.id = covid_data.id
        WHERE dates.date BETWEEN '2022-05-01' AND '2022-08-31'
        GROUP BY week
        ORDER BY week ASC
    """)
    weekly_covid_admissions = cur.fetchall()
    full_path = os.path.join(os.path.dirname(__file__), 'weekly_admissions.txt')
    file = open(full_path,'w')

    
    # Print the results
    file.write("Average weekly Covid admissions for each week from May 1, 2022 to August 31, 2022:\n")
    for week, avg in weekly_covid_admissions:
        file.write(f"Week {week}: {avg:.2f}\n")
    file.close()

    


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
    insert_covid_data(cur, conn, covid_dict)

     
    cur.execute("SELECT COUNT(*) FROM covid_data")
    rows = cur.fetchone()[0]
    print(rows)

    calculate_total_cases(cur)
    calculate_avg_weekly_covid_admissions(cur)
  
    conn.close()

if __name__ == '__main__':
    main()
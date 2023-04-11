import os
import requests
import pandas as pd
import sqlite3

# Task 1:

def create_database():
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+"final.database")
    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS flight_data (
        date TEXT,
        flight_number TEXT,
        scheduled_departure TEXT,
        actual_departure TEXT,
        status TEXT)''')
    conn.commit()
    return cur,conn

def get_flight_api(app_id, app_key, airport, start_date, end_date):
    url = "https://api.flightstats.com/flex/historical/rest/v3/json/flights"
    params = {"appId": app_id, "appKey": app_key, "airport": airport, "startDate": start_date, "endDate": end_date}
    response = requests.get(url, params=params)
    data = response.json()
    return data

def flight_data_table(cur, conn, data):
    flights = []
    for flight in data["flightStatuses"]:
        flights.append((
            flight["operationalTimes"]["actualGateDeparture"]["dateLocal"],
            flight["flightNumber"],
            flight["operationalTimes"]["scheduledGateDeparture"]["dateLocal"],
            flight["operationalTimes"]["actualGateDeparture"]["dateLocal"],
            flight["status"]
        ))
    # Insert the data into the database table
    cur.executemany("INSERT OR IGNORE INTO flight_data (date, flight_number, scheduled_departure, actual_departure, status) VALUES (?, ?, ?, ?, ?)", flights)
    conn.commit()



def main():
    # FlightStats API authentication information
    app_id = "37e401fc"
    app_key = "43875820d52e084e579102655ea9e923"

    # Location and date range for the flight data
    airport = "JFK"
    start_date = "2022-05-01"
    end_date = "2022-08-09"

    cur, conn = create_database()
    data = get_flight_api(app_id, app_key, airport, start_date, end_date)
    flight_data_table(cur, conn, data)
    conn.close()

if __name__ == "__main__":
    main()
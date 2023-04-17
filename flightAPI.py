import os
import requests
import sqlite3
from datetime import datetime, timedelta

# Task 1:

def create_database():
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+"final.database")
    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS flight_data (
        id INTEGER PRIMARY KEY,
        flight_number TEXT,
        scheduled_departure TEXT,
        actual_departure TEXT,
        delays INTEGER)''')
    conn.commit()
    return cur,conn

def get_flight_api(app_id, app_key, start_date):
    url = f"https://api.flightstats.com/flex/flightstatus/historical/rest/v3/json/route/status/JFK/ORD/dep/{start_date.year}/{start_date.month}/{start_date.day}?appId={app_id}&appKey={app_key}&hourOfDay=0&utc=false&numHours=24&maxFlights=5"
    response = requests.get(url)
    data = response.json()
    return data

def flight_data_table(cur, conn, data, id):
    count = 0
    if data == []:
        return None
    
    for flight in data["flightStatuses"]:
        date = flight["departureDate"]["dateLocal"]
        date = date.split("T")[0]

        # Check if Id is dates table
        cur.execute("SELECT id FROM dates WHERE date=?", (date,))
        row = cur.fetchone()
        if row is not None:
            id = row[0]
        else:
            print(date)
            continue
        
        flight_num= flight["flightNumber"]
        secheduled = flight["operationalTimes"]["scheduledGateDeparture"]["dateLocal"]
        try:
            actual = flight["operationalTimes"]["publishedDeparture"]["dateLocal"]
            if flight["delays"] != {}:
                if "departureRunwayDelayMinutes" in flight["delays"]:
                    delays= flight["delays"]["departureRunwayDelayMinutes"]
                else:
                    delays = 0
            else:
                delays = 0
        except:
            break
        
      
        # Check if a row already exists with the same date
        cur.execute("SELECT COUNT(*) FROM flight_data WHERE id=? AND flight_number=?", (id,flight_num))
        if cur.fetchone()[0] == 0:
            count += 1
     
        # Insert the data into the database table
        cur.execute("INSERT OR IGNORE INTO flight_data (id, flight_number, scheduled_departure, actual_departure, delays) VALUES (?, ?, ?, ?, ?)", (id, flight_num, secheduled, actual, delays))

        if count % 25 == 0:
            break
                     
    conn.commit()



def main():
# FlightStats API authentication information
    app_id = "c3295903"
    app_key = "b97f0560d6cefbd472b62ba5e1607d94"

# Location and date range for the flight data
    start_date= datetime(2022, 5, 1)
    end_date= datetime(2022, 8, 31)
    

    cur, conn = create_database()

    max_id = 0
    
    cur.execute("SELECT MAX(id) FROM flight_data")
    if len(cur.fetchall()) == 0:
        max_id = cur.fetchone()[0]
    start_date = start_date + timedelta(days=max_id)
    for id in range(5):
        data = get_flight_api(app_id, app_key, start_date)
        flight_data_table(cur, conn, data, id)
        start_date = start_date + timedelta(days=1)

    conn.close()

if __name__ == "__main__":
    main()
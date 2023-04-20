import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as ticker
import datetime
import sqlite3
import os

#Visualizations



#visualization 1- Bar chart showing the total number of COVID cases per month:
def bar_chart1(cur):
    # Get the total number of COVID cases per month
    cur.execute('''SELECT strftime('%m-%Y', dates.date) AS month, SUM(covid_data.weekly_new_cases) AS total_cases 
        FROM covid_data 
        JOIN dates ON covid_data.id = dates.id 
        GROUP BY month
        ''')
    covid_monthly_data = cur.fetchall()

    # Create a bar chart
    x_values = [data[0] for data in covid_monthly_data]
    y_values = [data[1] for data in covid_monthly_data]

    plt.bar(x_values, y_values)
    plt.xlabel("Month")
    plt.ylabel("Total COVID Cases")
    plt.title("Total COVID Cases per Month in New York")
    plt.show()



#visualization 2- Line chart showing the average temperature per month:
def bar_chart2(cur):
    # Get the average temperature per month
    cur.execute('''SELECT strftime('%m-%Y', dates.date) AS month, AVG(weather_data.avg_temp) AS avg_temp_monthly
                FROM dates JOIN weather_data ON dates.id = weather_data.id
                GROUP BY month''')
    temp_monthly_data = cur.fetchall()

    # Create a line chart
    x_values = [data[0] for data in temp_monthly_data]
    y_values = [data[1] for data in temp_monthly_data]

    plt.bar(x_values, y_values)
    plt.xlabel("Month")
    plt.ylabel("Average Temperature")
    plt.title("Average Temperature per Month in New York")
    plt.show()

#visualization 3- Scatter plot showing the relationship between the average temperature and the total number of COVID cases per month:
def sub_plot(cur):

    # Get the average temperature and total number of COVID cases per week
    cur.execute("""
        SELECT strftime('%Y-%m-%d', dates.date) AS date, AVG(weather_data.avg_temp) AS avg_temp, 
            SUM(covid_data.weekly_new_cases) AS total_cases
        FROM covid_data 
        JOIN weather_data ON covid_data.id = weather_data.id 
        JOIN dates ON covid_data.id = dates.id 
        WHERE dates.date BETWEEN '2022-05-01' AND '2022-08-31'
        GROUP BY date
    """)
    combined_data = cur.fetchall()

    # Split the data into three lists
    dates = [data[0] for data in combined_data]
    avg_temps = [data[1] for data in combined_data]
    total_cases = [data[2] for data in combined_data]

    # Create the graph with two y-axes
    fig, ax1 = plt.subplots()

    # Plot the average temperature line
    ax1.plot(dates, avg_temps, 'b-', label='Average Temperature')
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Average Temperature (°F)', color='b')

    # Create a second y-axis
    ax2 = ax1.twinx()

    # Plot the total cases line
    ax2.plot(dates, total_cases, 'r-', label='Total Cases')
    ax2.set_ylabel('Total Cases per Week', color='r')

    # Set x-axis labels and ticks
    ax1.set_xticks(range(0, len(dates), 14))
    ax1.set_xticklabels([dates[i][5:] for i in range(0, len(dates), 14)], rotation=45, ha='right')

    # Add the legend
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='best')

    # Set the title
    plt.title('Average Temperature and Total COVID Cases per Week (May-August 2022) in New York')

    # Show the graph
    plt.show()

#Visualization 4- Pie chart percentange of weather conditions based on weekly covid admissions 
def pie_chart(cur):
    # Get the total weekly COVID admissions for each weather condition
    cur.execute("""
        SELECT weather_data.condition, SUM(covid_data.weekly_covid_admissions) AS total_admissions 
        FROM covid_data 
        JOIN weather_data ON covid_data.id = weather_data.id 
        JOIN dates ON covid_data.id = dates.id 
        GROUP BY condition
    """)
    combined_data = cur.fetchall()

    # Create a pie chart
    labels = [data[0] for data in combined_data]
    sizes = [data[1] for data in combined_data]

    plt.pie(sizes, labels=labels, autopct='%1.1f%%')
    plt.title("Proportion of Weekly COVID Admissions per Weather Condition in New York")
    plt.show()


def main():
    # Connect to the database
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+'final.database')
    cur = conn.cursor()

    covid_cases= bar_chart1(cur)
    avg_temp= bar_chart2(cur)
    cases_temp= sub_plot(cur)
    admissons_weather= pie_chart(cur)

if __name__ == "__main__":
    main()
    
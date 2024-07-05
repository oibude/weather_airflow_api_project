import requests
import datetime
import logging 
import pandas as pd
import requests
import datetime
import logging 
import pandas as pd
from datetime import datetime, date
import os
from dotenv import load_dotenv
import requests
import psycopg2 as psy
from airflow.models import TaskInstance

#Access virtual environment for sensitive details
load_dotenv()

api_key= os.getenv('WEATHER_API')

# Define the API key, the city, and the base URL
from dotenv import load_dotenv
base_url= 'http://api.weatherstack.com/current'

locations= ['Edmonton', 'Calgary', 'Toronto', 'Vancouver', 'Winnipeg']
today_date = date.today()


def fetch_data(**kwargs):
    weather_data = []

    for location in locations:
        params = {
            "access_key": api_key,
            "query": location
        }

        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()  # Raise an error for non-200 status codes

            data = response.json()
            weather_data.append(data)

            logging.info(f"Fetched weather data successfully for {location}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch data for {location}: {e}")

    # Push fetched data to Xcom
    task_instance = TaskInstance(task=kwargs['task'], execution_date=kwargs['execution_date'])
    task_instance.xcom_push(key='weather_data', value=weather_data)
    
    return weather_data  

def transform_data(**kwargs):
    task_instance = TaskInstance(task=kwargs['task'], execution_date=kwargs['execution_date'])
    weather_data = task_instance.xcom_pull(task_ids='fetch_weather_data', key='weather_data')

    transformed_data = []

    for item in weather_data:
        try:
            if "location" in item and "current" in item:
                location = item["location"]["name"]
                country = item["location"]["country"]
                temperature = item["current"]["temperature"]
                temp_feels_like= item["current"]["feelslike"]
                wind_speed = item["current"]["wind_speed"]
                pressure = item["current"]["pressure"]
                visibility= item["current"]["visibility"]
                humidity = item["current"]["humidity"]
                local_time= item["location"]['localtime']
                observation_time = item["current"]["observation_time"]

                # Convert observation_time to time
                observation_time = datetime.strptime(observation_time, "%I:%M %p").time()
                local_time = datetime.strptime(local_time, "%Y-%m-%d %H:%M")

                transformed_data.append([location, country, temperature, temp_feels_like, wind_speed, pressure, visibility, humidity, local_time, observation_time])
            else:
                logging.warning(f"Skipping incomplete data for {weather_data.get('location', {}).get('name')}")
        except KeyError as e:
            logging.error(f"KeyError: {e} occurred while processing data for {weather_data.get('location', {}).get('name')}")
        except Exception as e:
            logging.error(f"An error occurred while processing data: {e}")

    # Define columns for the DataFrame
    columns = ["location", "country", "temperature", "temp_feels_like","wind_speed", "pressure", "visibility", "humidity", "local_time", "observation_time"]

    # Create DataFrame
    df = pd.DataFrame(transformed_data, columns=columns)

    return df

def load_data_to_db(first_time=True, **kwargs):

    task_instance = TaskInstance(task=kwargs['task'], execution_date=kwargs['execution_date'])
    transformed_data = task_instance.xcom_pull(task_ids='transform_weather_data')
    # Establish database connection
    conn = psy.connect(
        dbname= os.getenv('DB_NAME'),
        user= os.getenv('DB_USER'),
        host=os.getenv('DB_HOST'),
        password= os.getenv('PASSWORD'),
        port= os.getenv('DB_PORT')     
)

    try:
        with conn:
            with conn.cursor() as cur:
                if first_time:
                    # Create the table if it doesn't exist
                    cur.execute("""
                            CREATE TABLE IF NOT EXISTS weather (
                            id SERIAL PRIMARY KEY,
                            location VARCHAR(255) NOT NULL,
                            country VARCHAR(255) NOT NULL,
                            temperature DECIMAL NOT NULL,
                            temp_feels_like DECIMAL NOT NULL,
                            wind_speed DECIMAL NOT NULL,
                            pressure DECIMAL NOT NULL,
                            visibility DECIMAL NOT NULL,
                            humidity DECIMAL NOT NULL,
                            local_time TIMESTAMP NOT NULL,
                            observation_time TIME NOT NULL
                                    );
                    """)

                # Prepare data for insertion
                data_to_insert = transformed_data.values.tolist()

                # Insert data into the table
                cur.executemany("""
                    INSERT INTO weather (location, country, temperature, temp_feels_like, wind_speed, pressure, visibility,humidity, local_time, observation_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, data_to_insert)

        print("Data successfully loaded into PostgreSQL database.")
    except psy.Error as e:
        print(f"Error occurred while loading data to database: {e}")
    finally:
        # Close the connection
        conn.close()

# data= fetch_data(locations=locations)
# print(data)

#data_2= transform_data(data)
#print(data_2)

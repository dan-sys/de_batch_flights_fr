import os
import json
import sys
from airflow.sdk import task, dag, asset
import pendulum
import time
from dotenv import load_dotenv  
from TokenManager import TokenManager


# Add the project root (one level up from the DAGs folder) to sys.path
# so TokenManager.py at /opt/airflow/TokenManager.py can be imported.
sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
))

load_dotenv()  # Load environment variables from .env file

client_id = str(os.getenv("OPENSKY_CLIENT_ID"))
client_secret = str(os.getenv("OPENSKY_CLIENT_SECRET"))

tokens = TokenManager(client_id, client_secret)

@dag(
    dag_id="Airport_Arrivals_Departures",
    schedule=None,
    start_date=pendulum.datetime(2026, 8, 5, tz="Europe/Paris"),
    catchup=False,
    tags=["airportArrivalsDepartures"],
    default_args={
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=3),
    },
)

def pipeline_taskflow():
    """
    This is a simple data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    """
    
    @task()
    def extract_arrival_data_api(airports, startTime, endTime)-> list[dict]:
        """
        Extract task
        A simple Extract task to get data ready for the rest of the data
        pipeline. In this case, getting data is simulated by reading from a hardcoded JSON string.
        """
        import requests
        
        arrivals_all=[]

        for airport in airports:
            arrivals =f"https://opensky-network.org/api/flights/arrival?airport={airport}&begin={startTime}&end={endTime}"

            r = requests.get(arrivals,headers=tokens.headers(),)

            if r.status_code == 200:
                arrivals_data = r.json()
                arrivals_all.extend(arrivals_data)
            else:
                print(f"Error fetching data for airport {airport}: {r.status_code} - {r.text}")

        return arrivals_all
    
    @task()
    def extract_departure_data_api(airports, startTime, endTime)-> list[dict]:
        import requests
                
        departures_all=[]
        for airport in airports:
            departures = f"https://opensky-network.org/api/flights/departure?airport={airport}&begin={startTime}&end={endTime}"

            r = requests.get(departures,headers=tokens.headers(),)

            if r.status_code == 200:
                departures_data = r.json()
                departures_all.extend(departures_data)
            else:
                print(f"Error fetching data for airport {airport}: {r.status_code} - {r.text}")


        return departures_all


    @task()
    def load_arrival_data(arrivals_all: list[dict]) -> None:

        import psycopg2
        import polars as pl

        try:
            conn = psycopg2.connect(
                host="db",
                port=5432,
                dbname="db_flights",
                user="db_user",
                password="db_password"
            )
            
            cur = conn.cursor()
            
            # Create tables if they don't exist
            cur.execute("""
                CREATE SCHEMA IF NOT EXISTS dev;
                CREATE TABLE IF NOT EXISTS dev.raw_arrivals (
                    "icao24" TEXT,
                    "firstSeen" BIGINT,
                    "estDepartureAirport" TEXT,
                    "lastSeen" BIGINT,
                    "estArrivalAirport" TEXT,
                    "callsign" TEXT,
                    "estDepartureAirportHorizDistance" INT,
                    "estDepartureAirportVertDistance" INT,
                    "estArrivalAirportHorizDistance" INT,
                    "estArrivalAirportVertDistance" INT,
                    "departureAirportCandidatesCount" INT,
                    "arrivalAirportCandidatesCount" INT
                    PRIMARY KEY ("icao24","firstSeen")
                );
            """)

            insert_sql = """
            INSERT INTO opensky_flights (
                "icao24",
                "firstSeen",
                "estDepartureAirport",
                "lastSeen",
                "estArrivalAirport",
                "callsign",
                "estDepartureAirportHorizDistance",
                "estDepartureAirportVertDistance",
                "estArrivalAirportHorizDistance",
                "estArrivalAirportVertDistance",
                "departureAirportCandidatesCount",
                "arrivalAirportCandidatesCount"
            ) VALUES (
                %(icao24)s,
                %(firstSeen)s,
                %(estDepartureAirport)s,
                %(lastSeen)s,
                %(estArrivalAirport)s,
                %(callsign)s,
                %(estDepartureAirportHorizDistance)s,
                %(estDepartureAirportVertDistance)s,
                %(estArrivalAirportHorizDistance)s,
                %(estArrivalAirportVertDistance)s,
                %(departureAirportCandidatesCount)s,
                %(arrivalAirportCandidatesCount)s
            )
            ON CONFLICT ("icao24","firstSeen") DO UPDATE SET
            "lastSeen" = EXCLUDED."lastSeen",
            "estArrivalAirport" = EXCLUDED."estArrivalAirport",
            "estArrivalAirportHorizDistance" = EXCLUDED."estArrivalAirportHorizDistance",
            "estArrivalAirportVertDistance" = EXCLUDED."estArrivalAirportVertDistance";
            """ 

            for item in arrivals_all:
                params = {
                    "icao24": item.get("icao24"),
                    "firstSeen": item.get("firstSeen"),
                    "estDepartureAirport": item.get("estDepartureAirport"),
                    "lastSeen": item.get("lastSeen"),
                    "estArrivalAirport": item.get("estArrivalAirport"),
                    "callsign": item.get("callsign", "").strip() if item.get("callsign") else None,
                    "estDepartureAirportHorizDistance": item.get("estDepartureAirportHorizDistance"),
                    "estDepartureAirportVertDistance": item.get("estDepartureAirportVertDistance"),
                    "estArrivalAirportHorizDistance": item.get("estArrivalAirportHorizDistance"),
                    "estArrivalAirportVertDistance": item.get("estArrivalAirportVertDistance"),
                    "departureAirportCandidatesCount": item.get("departureAirportCandidatesCount"),
                    "arrivalAirportCandidatesCount": item.get("arrivalAirportCandidatesCount"),
                }
                cur.execute(insert_sql, params)

            # Commit the changes and close the cursor
            conn.commit()
            print("Arrival RawTables created  and inserted records successfully.")
            cur.close()
        except psycopg2.Error as e:
            print(f"Connection to PostgreSQL database failed: {e}")
            raise
        finally:
            if conn:
                cur.close()
                conn.close()
                print("PostgreSQL connection closed.")
        
            
    #arrivals_all = extract_arrival_data_api(airports, startTime, endTime)
    #departures_all = extract_departure_data_api(airports, startTime, endTime)
    airports = ["LFPG","LFPO","LFMN","LFLL","LFBO"]
        
    prev_day = pendulum.yesterday('Europe/Paris')
    prev_day_end = prev_day.end_of('day')
    fileDate = prev_day.strftime("%Y%m%d")
    startTime=str(prev_day.int_timestamp)
    endTime=str(prev_day_end.int_timestamp)

    arrivals = extract_arrival_data_api(airports, startTime, endTime)
    departures = extract_departure_data_api(airports, startTime, endTime)

    #load_arrival_data(arrivals_all=arrivals)  # type: ignore[arg-type]
    #load_departure_data(departures_all=departures)  # type: ignore[arg-type]
    # Equivalent dependency chain:

pipeline_taskflow()




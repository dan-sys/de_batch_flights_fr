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

            time.sleep(3)
            
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

            time.sleep(3)  # Sleep for 3 seconds to avoid hitting the rate limit


        return departures_all


    @task()
    def load_arrival_data(arrivals_all: list[dict]) -> None:

        import psycopg2
        import polars as pl

        conn = None
        cur = None
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
                    "arrivalAirportCandidatesCount" INT,
                    "raw_updated_at" TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY ("icao24","firstSeen")
                );
            """)

            insert_sql = """
            INSERT INTO dev.raw_arrivals (
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
            "estArrivalAirportVertDistance" = EXCLUDED."estArrivalAirportVertDistance",
            "raw_updated_at" = EXCLUDED."raw_updated_at";
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
                    "arrivalAirportCandidatesCount": item.get("arrivalAirportCandidatesCount")
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
            if cur:
                cur.close()
            if conn:
                conn.close()
                print("PostgreSQL connection closed.")


    @task()
    def load_departure_data(departures_all: list[dict]) -> None:
        import psycopg2

        conn = None
        cur = None
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
                CREATE TABLE IF NOT EXISTS dev.raw_departures (
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
                    "arrivalAirportCandidatesCount" INT,
                    "raw_updated_at" TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY ("icao24","firstSeen")
                );
            """)

            insert_sql = """
            INSERT INTO dev.raw_departures (
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
            "estArrivalAirportVertDistance" = EXCLUDED."estArrivalAirportVertDistance",
            "raw_updated_at" = EXCLUDED."raw_updated_at";
            """
            for item in departures_all:
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
            print("Departure RawTables created  and inserted records successfully.")
            cur.close()            
        except psycopg2.Error as e:
            print(f"Connection to PostgreSQL database failed: {e}")
            raise
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
                print("PostgreSQL connection closed.")            

    @task()
    def transform_raw_to_silver() -> None:
   
        import psycopg2
        conn = None
        cur = None

        conn = psycopg2.connect(
            host="db",
            port=5432,
            dbname="db_flights",
            user="db_user",
            password="db_password"
        )
        cur = conn.cursor()
    
        create_table_sql = """
            CREATE SCHEMA IF NOT EXISTS dev;
    
            CREATE TABLE IF NOT EXISTS dev.silver_arrivals (
                "icao24" TEXT NOT NULL,
                "callsign" TEXT,
                "airline_code" TEXT,
                "first_seen" TIMESTAMPTZ NOT NULL,
                "last_seen" TIMESTAMPTZ,
                "departure_airport" TEXT,
                "arrival_airport" TEXT,
                "flight_duration_seconds" BIGINT,
                "source_updated_at" TIMESTAMPTZ NOT NULL,
                PRIMARY KEY ("icao24", "first_seen")
            );
    
            CREATE TABLE IF NOT EXISTS dev.silver_departures (
                "icao24" TEXT NOT NULL,
                "callsign" TEXT,
                "airline_code" TEXT,
                "first_seen" TIMESTAMPTZ NOT NULL,
                "last_seen" TIMESTAMPTZ,
                "departure_airport" TEXT,
                "arrival_airport" TEXT,
                "flight_duration_seconds" BIGINT,
                "source_updated_at" TIMESTAMPTZ NOT NULL,
                PRIMARY KEY ("icao24", "first_seen")
            );
        """
    
        def upsert_silver(raw_table: str, silver_table: str) -> str:
            return f"""
                INSERT INTO {silver_table} AS target (
                    "icao24",
                    "callsign",
                    "airline_code",
                    "first_seen",
                    "last_seen",
                    "departure_airport",
                    "arrival_airport",
                    "flight_duration_seconds",
                    "source_updated_at"
                )
                SELECT
                    raw."icao24",
                    NULLIF(upper(btrim(raw."callsign")), '') AS "callsign",
                    SUBSTRING(NULLIF(upper(btrim(raw."callsign")), ''), 1, 3) AS "airline_code",
                    to_timestamp(raw."firstSeen") AS "first_seen",
                    to_timestamp(raw."lastSeen") AS "last_seen",
                    NULLIF(upper(btrim(raw."estDepartureAirport")), '') AS "departure_airport",
                    NULLIF(upper(btrim(raw."estArrivalAirport")), '') AS "arrival_airport",
                    CASE
                        WHEN raw."lastSeen" >= raw."firstSeen"
                        THEN raw."lastSeen" - raw."firstSeen"
                    END AS "flight_duration_seconds",
                    raw."raw_updated_at" AS "source_updated_at"
                FROM {raw_table} AS raw
                LEFT JOIN {silver_table} AS existing
                    ON existing."icao24" = raw."icao24"
                   AND existing."first_seen" = to_timestamp(raw."firstSeen")
                WHERE existing."icao24" IS NULL
                   OR raw."raw_updated_at" > existing."source_updated_at"
                ON CONFLICT ("icao24", "first_seen") DO UPDATE SET
                    "callsign" = EXCLUDED."callsign",
                    "airline_code" = EXCLUDED."airline_code",
                    "last_seen" = EXCLUDED."last_seen",
                    "departure_airport" = EXCLUDED."departure_airport",
                    "arrival_airport" = EXCLUDED."arrival_airport",
                    "flight_duration_seconds" =
                        EXCLUDED."flight_duration_seconds",
                    "source_updated_at" = EXCLUDED."source_updated_at"
                WHERE EXCLUDED."source_updated_at" > target."source_updated_at";
            """
    
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(create_table_sql)

                    for raw_table, silver_table in (
                        ("dev.raw_arrivals", "dev.silver_arrivals"),
                        ("dev.raw_departures", "dev.silver_departures"),
                    ):
                        cur.execute(upsert_silver(raw_table, silver_table))
        finally:
            conn.close()

    @task()
    def transform_silver_to_gold() -> None:
        import psycopg2
        conn = None
        cur = None

        
        try:
            conn = psycopg2.connect(
                host="db",
                port=5432,
                dbname="db_flights",
                user="db_user",
                password="db_password"
            )
            cur = conn.cursor()

            with conn:
                with conn.cursor() as cur:
                    cur.execute("""
                    CREATE SCHEMA IF NOT EXISTS dev;

                    CREATE TABLE IF NOT EXISTS dev.gold_airport_daily (
                        "report_date" DATE NOT NULL,
                        "airport" TEXT NOT NULL,
                        "direction" TEXT NOT NULL
                            CHECK ("direction" IN ('arrival', 'departure')),
                        "airline_code" TEXT NOT NULL,
                        "flight_count" BIGINT NOT NULL,
                        "avg_duration_seconds" NUMERIC(12, 2),
                        "max_duration_seconds" BIGINT,
                        "source_updated_at" TIMESTAMPTZ NOT NULL,
                        PRIMARY KEY (
                            "report_date",
                            "airport",
                            "direction",
                            "airline_code"
                        )
                    );
                    """)

                    cur.execute("""
                    TRUNCATE TABLE dev.gold_airport_daily;

                    WITH flight_events AS (
                        SELECT
                            (first_seen AT TIME ZONE 'Europe/Paris')::date
                                AS report_date,
                            arrival_airport AS airport,
                            'arrival'::text AS direction,
                            COALESCE(airline_code, 'UNKNOWN') AS airline_code,
                            flight_duration_seconds,
                            source_updated_at
                        FROM dev.silver_arrivals
                        WHERE arrival_airport IS NOT NULL

                        UNION ALL

                        SELECT
                            (first_seen AT TIME ZONE 'Europe/Paris')::date
                                AS report_date,
                            departure_airport AS airport,
                            'departure'::text AS direction,
                            COALESCE(airline_code, 'UNKNOWN') AS airline_code,
                            flight_duration_seconds,
                            source_updated_at
                        FROM dev.silver_departures
                        WHERE departure_airport IS NOT NULL
                    )
                    INSERT INTO dev.gold_airport_daily (
                        "report_date",
                        "airport",
                        "direction",
                        "airline_code",
                        "flight_count",
                        "avg_duration_seconds",
                        "max_duration_seconds",
                        "source_updated_at"
                    )
                    SELECT
                        report_date,
                        airport,
                        direction,
                        airline_code,
                        COUNT(*) AS flight_count,
                        ROUND(AVG(flight_duration_seconds)::numeric, 2)
                            AS avg_duration_seconds,
                        MAX(flight_duration_seconds) AS max_duration_seconds,
                        MAX(source_updated_at) AS source_updated_at
                    FROM flight_events
                    GROUP BY
                        report_date,
                        airport,
                        direction,
                        airline_code;
                    """)
        finally:
            if conn is not None:
                conn.close()





    airports = ["LFPG",
     "LFPO", "LFBO", "LFLL", "LFML", "LFMN", "LFMT", 
     "LFMV", "LFBD", "LFBD", "LFBZ", "LFBH", "LFBK", "LFBL", 
     "LFBM", "LFBN", "LFBO", "LFBP", "LFBQ", "LFBR", "LFBS", 
     "LFBT", "LFBU", "LFBV", "LFBW", "LFBX", "LFBY"]
        
    prev_day = pendulum.yesterday('Europe/Paris')
    prev_day_end = prev_day.end_of('day')
    fileDate = prev_day.strftime("%Y%m%d")
    startTime=str(prev_day.int_timestamp)
    endTime=str(prev_day_end.int_timestamp)

    arrivals = extract_arrival_data_api(airports, startTime, endTime)
    departures = extract_departure_data_api(airports, startTime, endTime)

    arrival_load = load_arrival_data(arrivals_all=arrivals)  # type: ignore[arg-type]

    departure_load = load_departure_data(departures_all=departures)  # type: ignore[arg-type]
    # Equivalent dependency chain:
    silver_transform = transform_raw_to_silver()  # type: ignore[arg-type]

    arrival_load.set_downstream(silver_transform)  # Ensure that the transformation runs after the arrival load
    departure_load.set_downstream(silver_transform)  # Ensure that the transformation runs after the departure load

    silver_transform.set_downstream(transform_silver_to_gold())  # Ensure that the gold transformation runs after the silver transformation 



pipeline_taskflow()




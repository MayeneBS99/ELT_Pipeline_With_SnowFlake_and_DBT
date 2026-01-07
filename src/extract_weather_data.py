import os
import requests
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()

URL = "https://api.openweathermap.org/data/2.5/weather"
CITY = "Paris"

params = {
    "q": CITY,
    "appid": os.getenv("WEATHER_API_KEY"),
    "units": "metric"  # Celsius
}


def extract_weather(snapshot_ts):

    print(f"Extraction weather data Initialization .......")

    response = requests.get(URL, params = params)
    if response.status_code == 200 :
        data = response.json()
        df = pd.json_normalize(data)
        df["snapshot_ts"] = snapshot_ts
        print(f"Succes API ! dataframe : {df.shape[0]} lines and {df.shape[1]} columns")
        return df
    else :
        print(f"ERROR API : {requests.status_codes}")
        return None


def load_to_snowflake2(df,name_table):

    print("Loading in Snowflake ............")
    try :
        connector = snowflake.connector.connect(
        user = os.getenv("SNOWFLAKE_USER"),
        account = os.getenv("SNOWFLAKE_ACCOUNT"),
        password = os.getenv("SNOWFLAKE_PASSWORD"),
        database = os.getenv("SNOWFLAKE_DATABASE"),
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE"),
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        schema = "RAW"
        )
        
        succes, nchunks, nrows,_ = write_pandas(connector, df, table_name = name_table, auto_create_table = True) 
        print(f"Succes ! {nrows} lines loaded in {name_table}.")

    except Exception as e :
        print(f"Error in loading : {e}")
    finally :
        connector.close()
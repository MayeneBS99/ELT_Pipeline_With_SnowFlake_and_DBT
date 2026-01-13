import os
import requests
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()

url_station_status = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json"
url_information_status = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json"

def extract_data_velib_status(snapshot_ts) :

    print(f"Extraction velib station status Initialization .......")

    response = requests.get(url_station_status)
    if response.status_code == 200 :
        data = response.json()
        df = pd.json_normalize(data['data']['stations'])
        df["snapshot_ts"] = snapshot_ts
        print(f"Succes API ! dataframe : {df.shape[0]} lines and {df.shape[1]} columns")

        return df
    else :
        print(f"ERROR API : {requests.status_codes}")
        return None


def extract_data_station_information(snapshot_ts) :

    print(f"Extraction velib station information Initialization .......")

    response = requests.get(url_information_status)
    if response.status_code == 200 :
        data = response.json()
        df = pd.json_normalize(data['data']['stations'])
        df["snapshot_ts"] = snapshot_ts
        print(f"Succes API ! dataframe : {df.shape[0]} lines and {df.shape[1]} columns")

        return df
    else :
        print(f"ERROR API : {requests.status_codes}")
        return None



def load_to_snowflake(df,name_table):

    print("Loading in Snowflake ............")
    print(os.getenv("SNOWFLAKE_USER"))

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
        
        succes, nchunks, nrows,_ = write_pandas(connector, df, name_table, auto_create_table = True) 
        print(f"Succes ! {nrows} lines loaded in {name_table}.")
    except Exception as e :
        print(f"Error in loading : {e}")
    finally :
        connector.close()

        

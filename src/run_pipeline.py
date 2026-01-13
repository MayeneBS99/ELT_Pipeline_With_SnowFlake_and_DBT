import pandas as pd
import pyarrow
from src.extract_velib_data import extract_data_velib_status, extract_data_station_information, load_to_snowflake
from src.extract_weather_data import extract_weather, load_to_snowflake2

if __name__ == "__main__" : 

    SNAPSHOT_TS = pd.Timestamp.utcnow()

    df_status = extract_data_velib_status(SNAPSHOT_TS)
    df_info = extract_data_station_information(SNAPSHOT_TS)
    df_weather = extract_weather(SNAPSHOT_TS)

    load_to_snowflake(df_status, "VELIB_STATION_STATUS_RAW" )
    load_to_snowflake(df_info, "VELIB_STATION_INFORMATION_RAW" )
    load_to_snowflake2(df_weather, "RAW_WEATHER")
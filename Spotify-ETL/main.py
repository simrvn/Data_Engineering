#import all libraries
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import pandas as pd
import requests
import json
from datetime import datetime
import datetime
import sqlite3

#declare and initialise working variables/constants
DATABASE_LOCATION = "sqlite://played_tracks.sqlite"
USER_ID = "simrvn"
TOKEN = "" #insert API token


'''
    The following includes functions to check for validity of data. Some of the steps include checking for
    1. Checking if dataframe is empty (data for a particular day is not present i.e. did not listen to songs for 
        that day)
    2. Check for duplicates using primary key which is played_at_time 
    3. Check for nulls (In this project, null values in any row or column provide us with no use, hence we will raise
        an Exception if any)
'''

def check_for_valid_data(df: pd.DataFrame) -> bool:

    #check if dataframe is empty
    if df.empty:
        print("No songs found and downloaded. Finishing Execution")
        return False
    #check for duplicates
    if pd.Series(df['played_at_time']).is_unique:
        pass
    else:
        raise Exception("Primary Key Check is violated")

    #check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")

    #check that all time stamps are of relevant and correct date (within last 24 hours)
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df["timestamps"].tolist()
    for time in timestamps:
        if datetime.datetime.strptime(time, "%Y-%m-%d") != yesterday:
            raise Exception('There is at least one song that is not played within the last 24 hours')

    return True

'''
    Main Function that consists of the following steps:
    1. Scrape data using API
    2. Extract relevant fields from json object
    3. Store in a pandas DataFrame
'''

if __name__ == "__main__":

    '''
    The first step in the ETL Process is to 'Extract' the data. This is done using the Spotify API.
    Data extracted will be songs listened in the last 24 hours.
    Note: Spotify limits to 50 songs.
    '''
    #based on Spotify API's instructions
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=TOKEN)
    }

    #Yesterday and today -> to retrieve all songs played within last 24 hours
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    #request
    req = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers=headers)

    data = req.json()

    print(data)

    #list of fields that we want for analysis

    song_names = []
    artist_names = []
    played_at_time = []
    timestamps = []

    #for songs in data["items"]:
    # #store in a dataframe

    '''
    The next step in the ETL process is to 'Transform' the data. 
    Data Transformation in this project mainly focuses on validating data that is being extract. 
    This is done using the 'check_for_valid_data' function which was elaborated above
    '''

    if check_for_valid_data(songs_df):
        print("All data is valid, proceed to Load Stage")

    '''
    The last step is to 'Load' and store the data into a database. For the purposes of this project,
    sqlite and sqlalchemy is used solely because it is straightforward.
    '''

    #create engine and connect to database
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('played_tracks.sqlite')
    cursor = conn.cursor()

    sql_query = """
        CREATE TABLE IF NOT EXISTS played_tracks(
            song_name VARCHAR(200),
            artist_name VARCHAR(200),
            played_at_time VARCHAR(200),
            time_stamp VARCHAR(200),
            CONSTRAINT primary_key_constraint PRIMARY KEY (played_at_time)
        )
    """
    cursor.execute(sql_query)
    print('Opened Database Successfully')

    try:
        songs_df.to_sql("played_tracks", engine, index=False, if_exists='append')
    except:
        print('Data already exists in the database')

    conn.close()
    print("Connection Closed Successfully")

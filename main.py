import sqlalchemy
import pandas as pd
import requests
from datetime import datetime
import datetime
import sqlite3
import json
from sqlalchemy.orm import sessionmaker


def is_data_valid(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No songs found!")
        return False

    # Primary key check
    if pd.Series(df["played_at"]).is_unique:
        pass
    else:
        raise Exception("Primary keys are not unique!")

    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null value found!")

    # Check if all timestamps are of yesterday's date
    yesterday = datetime.datetime.now()  # today at midnight
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    timestamps = df["timestamp"].tolist()

    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp[:10], "%Y-%m-%d") != yesterday:
            raise Exception("One of the songs doesn't come from the last 24 hours!")

    return True


def run_spotify_etl():
    database_location = "sqlite:///my_played_tracks.sqlite"
    token = "GENERATED_TOKEN"

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=token)
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    # extracting data from the playlist
    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers=headers)

    data = r.json()
    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    # transforming
    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"])

    song_dict = {
        "song_name": song_names,
        "artist_name": artist_names,
        "played_at": played_at_list,
        "timestamp": timestamps
    }

    song_df = pd.DataFrame(song_dict, columns=["song_name", "artist_name", "played_at", "timestamp"])

    # data validation
    if is_data_valid(song_df):
        print("Data is valid, proceeding...")
    else:
        raise Exception("Data is NOT valid!")

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)
    print(song_df)

    # adding songs to the SQLite database
    engine = sqlalchemy.create_engine(database_location)
    conn = sqlite3.connect("my_played_tracks.sqlite")
    cursor = conn.cursor()

    sql_query = """
        CREATE TABLE IF NOT EXISTS my_played_tracks(
            song_name VARCHAR(200),
            artist_name VARCHAR(200),
            played_at VARCHAR(200),
            timestamp VARCHAR(200),
            CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
        )
    """

    cursor.execute(sql_query)

    try:
        song_df.to_sql("my_played_tracks", engine, index=False, if_exists="append")
    except:
        print("Data is already in the database!")

    conn.close()
    print("Connection closed!")
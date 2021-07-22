import sqlalchemy
from sqlalchemy.orm import sessionmaker
import pandas as pd
import requests
import json
from datetime import datetime
import datetime
import sqlite3

DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
USER_ID = "pfb1novtdnuiyu990g9eaya1a"
TOKEN = "BQCJioPqgAX13DkYTR18B-_yJMK7J35cZZ3VZUw8X5xa2HBn16hdU5uVLrAttSwLO0vodyUMv0tF6qICJP4aNM3VybEvkNGqIYD64N8K-hDIBZLK0EysiQctvJP0at-9P0tRNAfgjLzeoxI7KDVb97haV-1IjmlCCaXu"

if __name__ == "__main__":
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=TOKEN)
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    r = requests.get("https://api.spotify.com/v1/me/player/recently-played", headers=headers)

    data = r.json()
    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

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
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)
    print(song_df)
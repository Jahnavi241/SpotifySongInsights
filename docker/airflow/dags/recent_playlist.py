import time
import pandas as pd
import requests

access_token = "*****"

def recent_played_songs():
    header_variables = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}"
    }

    unix_timestamp = int((time.time() - 3600) * 1000)
    # Download all songs listened to in the last 1 hour
    response = requests.get(
        "https://api.spotify.com/v1/me/player/recently-played?limit=50&after={time}".format(time=unix_timestamp),
        headers=header_variables)

    if response.status_code != 200:
        raise f"Error: {response.status_code}, {response.json()}"

    songs_data = response.json()
    song_names = []
    artist_names = []
    track_numbers = []
    song_popularity = []
    time_song_played = []
    date_song_played = []

    for song in songs_data["items"]:  # Extract relevant data from the JSON object
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        track_numbers.append(song["track"]["track_number"])
        song_popularity.append(song["track"]["popularity"])
        time_song_played.append(song["played_at"])
        date_song_played.append(song["played_at"][0:10])

    song_dict = {  # Create a dictionary to convert it into a pandas DataFrame
        "song_name": song_names,
        "artist_name": artist_names,
        "track_number": track_numbers,
        "popularity": song_popularity,
        "played_time": time_song_played,
        "played_date": date_song_played
    }

    songs_df = pd.DataFrame(song_dict, columns=["song_name", "artist_name", "track_number", "popularity", "played_time", "played_date"])
    return songs_df

    
def data_quality_checks(songs_df):
    if songs_df.empty:      # Check if the DataFrame is empty
        print("No Songs Extracted")
        return False

    if not songs_df["played_time"].is_unique:        # Check for duplicates in the "played_time" column (primary key enforcement)
        raise ValueError("Primary Key Violation: Data might contain duplicates.")

    if songs_df.isnull().any().any():       # Check for any null values in the DataFrame
        raise ValueError("Data contains null values.")

    print("Data Quality Check Passed")
    return True


def spotify_datapipeline():
    songs_df=recent_played_songs()
    print(songs_df)
    data_quality_checks(songs_df)
    return songs_df


if __name__ == '__main__':
    spotify_datapipeline()
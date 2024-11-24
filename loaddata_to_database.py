import os

from sqlalchemy.exc import IntegrityError

from get_spotify_data import recent_played_songs
from data_transformation import data_quality_checks, data_validation
import sqlalchemy
import sqlite3
from dotenv import load_dotenv


load_dotenv()
database_location = os.getenv("DATABASE_LOCATION")


def query_execution(recent_songs_df, popular_songs_df):
    engine = sqlalchemy.create_engine(database_location)        #creating database connnection
    conn = sqlite3.connect('recent_popular_tracks.sqlite')
    cursor = conn.cursor()

    # SQL query to create a list of the recently played tracks
    sql_query_1 = """   
        CREATE TABLE IF NOT EXISTS recent_playlists(
            id INTEGER PRIMARY KEY AUTOINCREMENT,  -- Unique ID for each row
            song_name VARCHAR(200),
            artist_name VARCHAR(200),
            track_number INT,
            popularity INT,
            played_time VARCHAR(200),
            played_date VARCHAR(200)
        )
        """
    # SQL query to create a list of the popular tracks from recently played tracks
    sql_query_2 = """
        CREATE TABLE IF NOT EXISTS popular_playlists(
            song_name VARCHAR(200),
            artist_name VARCHAR(200),
            popularity INT,
            played_time VARCHAR(200),
            count INT,
            popular BOOLEAN,
            CONSTRAINT primary_key_constraint PRIMARY KEY (song_name, artist_name)
        )
        """
    cursor.execute(sql_query_1)
    cursor.execute(sql_query_2)
    print("Opened database successfully")
    # cursor.execute('DELETE FROM recent_playlists')
    # conn.commit()
    # cursor.execute('DELETE FROM popular_playlists')
    # conn.commit()

    try:
        # Append only new data to avoid duplicates
        recent_songs_df.to_sql("recent_playlists", engine, index=False, if_exists='append')
    except IntegrityError as e:
        print("Data already exists in the 'recent_playlists' table:", e)
    except Exception as e:
        print("An error occurred while inserting into 'recent_playlists':", e)

    try:
        # Append only new data to avoid duplicates
        popular_songs_df.to_sql("popular_playlists", engine, index=False, if_exists='append')
    except IntegrityError as e:
        print("Data already exists in the 'popular_playlists' table:", e)
    except Exception as e:
        print("An error occurred while inserting into 'popular_playlists':", e)


    conn.close()
    print("Close database successfully")


def data_extraction():
    recent_songs_df = recent_played_songs()

    if not data_quality_checks(recent_songs_df):
        raise "Failed at Data Validation"

    popular_songs_df = data_validation(recent_songs_df)
    query_execution(recent_songs_df, popular_songs_df)


if __name__ == "__main__":
    data_extraction()
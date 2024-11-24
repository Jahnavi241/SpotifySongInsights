from recent_playlist import recent_played_songs, data_quality_checks


def data_validation(songs_df):
    updated_songs_df = songs_df.groupby(["song_name", "artist_name"], as_index=False).agg(
        played_time=("played_time", "max"),  # Get the latest played_time
        count=("played_time", "size"),  # Count the number of times the song was played
        popularity=("popularity", "max")  # Get the maximum popularity for the song
    )

    updated_songs_df.rename(columns={"played_at": "count"}, inplace=True)
    updated_songs_df = updated_songs_df[["song_name", "artist_name", "popularity", "played_time", "count"]]
    updated_songs_df["popular"] = updated_songs_df["popularity"] >= 60
    return updated_songs_df


def popular_songs_pipeline():
    songs_df = recent_played_songs()
    print(songs_df)

    data_quality_checks(songs_df)
    popular_songs = data_validation(songs_df)
    print(popular_songs)
    return popular_songs


if __name__ == '__main__':
    popular_songs_pipeline()
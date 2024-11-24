def data_quality_checks(songs_df):
    if songs_df.empty:      # Check if the DataFrame is empty
        print("No Songs Extracted")
        return False

    if not songs_df["played_time"].is_unique:         # Check for duplicates in the "played_time" column (primary key enforcement)
        raise ValueError("Primary Key Violation: Data might contain duplicates.")

    if songs_df.isnull().any().any():           # Check for any null values in the DataFrame
        raise ValueError("Data contains null values.")

    print("Data Quality Check Passed")
    return True


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
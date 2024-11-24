from data_transformation import data_quality_checks, data_validation
from get_spotify_data import recent_played_songs


def spotify_datapipeline():
    songs_df=recent_played_songs()
    print(songs_df)

    data_quality_checks(songs_df)
    popular_songs=data_validation(songs_df)
    print(popular_songs)
    return popular_songs


if __name__ == '__main__':
    spotify_datapipeline()



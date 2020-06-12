import json

import pandas as pd


def getData():
    user_rated_movies = pd.read_table("../user_ratedmovies.dat")
    movie_genres = pd.read_table("../movie_genres.dat")
    movie_genres['dummy'] = int(1)
    data = pd.DataFrame(movie_genres.pivot_table(index='movieID', columns='genre', values='dummy').fillna(0))
    user_genre_rating = pd.merge(data, user_rated_movies[['movieID', 'userID', 'rating']], on="movieID")
    return user_genre_rating, data, data.columns


def getRow(user_genre_rating, index):
    return json.loads(user_genre_rating.to_json(orient='records'))[index]


# def get_this_shit(nrows):
#     DFUserRatedMovies = pd.read_csv('../user_ratedmovies.dat', header=0, delimiter='\t',
#                                     usecols=['userID', 'movieID', 'rating'], nrows=nrows)
#     DFMovieGenres = pd.read_csv('../movie_genres.dat', header=0, delimiter="\t")
#     DFMovieGenres['dummyColumn'] = int(1)
#     DFMovieGenresPivoted = DFMovieGenres.pivot_table(index='movieID', columns='genre', values='dummyColumn')
#     DFMovieGenresPivotedColumnNamesMap = {}
#     genres_column_names = []
#     for DFMovieGenresPivotedColumnName in DFMovieGenresPivotedColumnNamesMap:
#         another_genre_column_name = 'gnere-' + DFMovieGenresPivotedColumnName
#         DFMovieGenresPivotedColumnNamesMap[DFMovieGenresPivotedColumnName] = another_genre_column_name
#         genres_column_names.append(another_genre_column_name)
#     DFMovieGenresPivoted = DFMovieGenresPivoted.rename(columns=DFMovieGenresPivotedColumnNamesMap)
#     DFUserRatedMoviesGenres = pd.merge(DFUserRatedMovies, DFMovieGenresPivoted, on='movieID')
#     return DFUserRatedMoviesGenres, genres_column_names


if __name__ == "__main__":
    print(getData())

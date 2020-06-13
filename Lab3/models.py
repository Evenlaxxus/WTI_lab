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


if __name__ == "__main__":
    print(getData())

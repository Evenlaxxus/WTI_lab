import pandas as pd
import numpy as np


def zad1():
    user_rated_movies = pd.read_table("../user_ratedmovies.dat", nrows=100)
    movie_genres = pd.read_table("../movie_genres.dat")
    movie_genres['dummy'] = int(1)
    data = pd.DataFrame(movie_genres.pivot_table(index='movieID', columns='genre', values='dummy').fillna(0))
    user_genre_rating = pd.merge(data, user_rated_movies[['movieID', 'userID', 'rating']], on="movieID")
    return user_genre_rating, data.columns


def zad2(df):
    return df.T.to_dict().values()


def zad3(list):
    return pd.DataFrame(list)


def zad4():
    user_rated_movies, genres = zad1()
    print(zad2(user_rated_movies))
    print(zad3(zad2(user_rated_movies)))


if __name__ == "__main__":
    zad4()

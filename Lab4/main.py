import pandas as pd


def zad1():
    user_rated_movies = pd.read_table("../user_ratedmovies.dat", nrows=100)
    movie_genres = pd.read_table("../movie_genres.dat")
    movie_genres['dummy'] = int(1)
    data = pd.DataFrame(movie_genres.pivot_table(index='movieID', columns='genre', values='dummy'))
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


def zad5():
    user_rated_movies, genres = zad1()
    genre_ratings = {}
    for genre in genres:
        ratings = []
        for index, row in user_rated_movies.iterrows():
            if row[genre] == 1.0:
                ratings.append(row['rating'])
        if len(ratings) != 0:
            genre_ratings[genre] = sum(ratings) / len(ratings)
        else:
            genre_ratings[genre] = 0
    return genre_ratings


def zad6(userID):
    user_rated_movies, genres = zad1()
    genre_user_ratings = {"userID": userID}
    for genre in genres:
        ratings = []
        for index, row in user_rated_movies.iterrows():
            if row['userID'] == userID:
                if row[genre] == 1.0:
                    ratings.append(row['rating'])
        if len(ratings) != 0:
            genre_user_ratings[genre] = sum(ratings) / len(ratings)
        else:
            genre_user_ratings[genre] = 0
    return genre_user_ratings


if __name__ == "__main__":
    # zad4()
    # print(zad5())
    print(zad6(78))


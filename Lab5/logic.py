import pandas as pd


def readData():
    user_rated_movies = pd.read_table("../user_ratedmovies.dat", nrows=100)
    movie_genres = pd.read_table("../movie_genres.dat")
    movie_genres['dummy'] = int(1)
    data = pd.DataFrame(movie_genres.pivot_table(index='movieID', columns='genre', values='dummy'))
    user_genre_rating = pd.merge(data, user_rated_movies[['movieID', 'userID', 'rating']], on="movieID")
    return user_genre_rating, data.columns


class Controller:
    def __init__(self):
        self.data, self.genres = readData()

    @staticmethod
    def df_to_dict(df):
        return df.T.to_dict().values()

    @staticmethod
    def list_to_df(dict_list):
        return pd.DataFrame(dict_list)

    def print_df_and_list(self):
        user_rated_movies, genres = self.data, self.genres
        print(self.df_to_dict(user_rated_movies))
        print(self.list_to_df(self.df_to_dict(user_rated_movies)))

    def genre_ratings(self):
        user_rated_movies, genres = self.data, self.genres
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

    def genre_user_ratings(self, userID):
        user_rated_movies = self.data
        genre_user_ratings = {"userID": userID}
        for genre in self.genres:
            ratings = []
            for index, row in user_rated_movies.iterrows():
                if int(row['userID']) == int(userID):
                    if row[genre] == 1.0:
                        ratings.append(row['rating'])
            if len(ratings) != 0:
                genre_user_ratings[genre] = sum(ratings) / len(ratings)
            else:
                genre_user_ratings[genre] = 0
        return genre_user_ratings

    def user_profile(self, userID):
        user_rated_movies, genres = self.data, self.genres
        genre_ratings = self.genre_ratings()
        genre_user_ratings = self.genre_user_ratings(userID)
        user_profile = {'userID': userID}
        for genre in genres:
            user_profile[genre] = genre_user_ratings[genre] - genre_ratings[genre]
        return user_profile

    def addRow(self, row):
        self.data = pd.concat([self.data, pd.json_normalize(row)])
        return row

    def deleteRow(self, request_data):
        indexNames = self.data[
            (self.data['movieID'] == request_data['movieID']) & (self.data['userID']
                                                                 == request_data['userID'])].index
        print(indexNames)
        self.data.drop(indexNames, inplace=True)
        return 'ok'

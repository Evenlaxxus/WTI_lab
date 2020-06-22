import pandas as pd
from elasticsearch import Elasticsearch, helpers
import numpy as np
import random


class ElasticClient:
    def __init__(self, address='localhost:10000'):
        self.es = Elasticsearch(address)

    # ------ Simple operations ------
    def index_documents(self):
        df = pd.read_csv('../user_ratedmovies.dat', delimiter='\t').loc[:, ['userID', 'movieID', 'rating']]
        means = df.groupby(['userID'], as_index=False, sort=False).mean().loc[:, ['userID', 'rating']].rename(
            columns={'rating': 'ratingMean'})
        df = pd.merge(df, means, on='userID', how="left", sort=False)
        df['ratingNormal'] = df['rating'] - df['ratingMean']
        ratings = df.loc[:, ['userID', 'movieID', 'ratingNormal']].rename(
            columns={'ratingNormal': 'rating'}).pivot_table(index='userID', columns='movieID', values='rating').fillna(
            0)
        print("Indexing users...")
        index_users = [{
            "_index": "users",
            "_type": "user",
            "_id": index,
            "_source": {
                'ratings': row[row > 0].sort_values(ascending=False).index.values.tolist()
            }
        } for index, row in ratings.iterrows()]
        helpers.bulk(self.es, index_users)
        print("Done")
        print("Indexing movies...")
        index_movies = [{
            "_index": "movies",
            "_type": "movie",
            "_id": column,
            "_source": {
                "whoRated": ratings[column][ratings[column] > 0].sort_values(ascending=False).index.values.tolist()
            }
        } for column in ratings]
        helpers.bulk(self.es, index_movies)
        print("Done")

    def get_movies_liked_by_user(self, user_id, index='users'):
        user_id = int(user_id)
        return self.es.get(index=index, doc_type="user", id=user_id)["_source"]

    def get_users_that_like_movie(self, movie_id, index='movies'):
        movie_id = int(movie_id)
        return self.es.get(index=index, doc_type="movie", id=movie_id)["_source"]

    def get_user_preselection(self, user_id, index='users'):
        user_id = int(user_id)
        movies_rated_by_user = self.es.search(index=index, body={
            "query": {
                "term": {
                    "_id": user_id
                }
            }
        })["hits"]["hits"][0]["_source"]["ratings"]

        users_that_rated_at_least_one_movie = self.es.search(index=index, body={
            "query": {
                "terms": {
                    "ratings": movies_rated_by_user
                }
            }
        })["hits"]["hits"]

        unique_movies = set()
        for ratings in users_that_rated_at_least_one_movie:
            if ratings["_id"] != user_id:
                ratings = ratings["_source"]["ratings"]
                for rating in ratings:
                    if rating not in movies_rated_by_user:
                        unique_movies.add(rating)

        return list(unique_movies)

    def get_movies_preselection(self, movie_id, index='movies'):
        movie_id = int(movie_id)
        users_that_rated_movie = self.es.search(index=index, body={
            "query": {
                "term": {
                    "_id": movie_id
                }
            }
        })["hits"]["hits"][0]["_source"]["whoRated"]

        movies_rated_by_at_least_one_of_users = self.es.search(index=index, body={
            "query": {
                "terms": {
                    "whoRated": users_that_rated_movie
                }
            }
        })["hits"]["hits"]

        unique_users = set()
        for ratings in movies_rated_by_at_least_one_of_users:
            if ratings["_id"] != movie_id:
                ratings = ratings["_source"]["whoRated"]
                for rating in ratings:
                    if rating not in users_that_rated_movie:
                        unique_users.add(rating)

        return list(unique_users)

    def add_user_document(self, user_id, movies_liked_by_user, user_index="users", movie_index="movies"):
        user_id = int(user_id)
        movies = list(set(movies_liked_by_user))
        to_update = [self.es.get(index=movie_index, id=movie_id, doc_type='movie') for movie_id in movies]

        if len(to_update) != len(movies):
            raise Exception("Some movies are unknown!")

        for movie_document in to_update:
            users = movie_document["_source"]["whoRated"]
            users.append(user_id)
            users = list(set(users))
            self.es.update(index=movie_index, id=movie_document["_id"], doc_type='movie', body={
                "doc": {
                    "whoRated": users
                }
            })

        self.es.create(index=user_index, id=user_id, body={"ratings": movies}, doc_type='user')

    def add_movie_document(self, movie_id, users_who_like_movie, user_index="users", movie_index="movies"):
        movie_id = int(movie_id)
        users = list(set(users_who_like_movie))
        to_update = [self.es.get(index=user_index, id=user_id, doc_type='user') for user_id in users]

        if len(to_update) != len(users):
            raise Exception("Some users are unknown")

        for user_document in to_update:
            movies = user_document["_source"]["ratings"]
            movies.append(movie_id)
            movies = list(set(movies))
            self.es.update(index=user_index, id=user_document["_id"], doc_type='user', body={
                "doc": {
                    "ratings": movies
                }
            })

        self.es.create(index=movie_index, id=movie_id, body={"whoRated": users}, doc_type='movie')

    def update_user_document(self, user_id, movies_liked_by_user, user_index="users", movie_index="movies"):
        user_id = int(user_id)
        movies = list(set(movies_liked_by_user))
        to_update = self.es.get(index=user_index, id=user_id, doc_type='user')
        old_movies = to_update['_source']['ratings']

        movies_to_add_user = np.setdiff1d(movies, old_movies)
        movies_to_remove_user = np.setdiff1d(old_movies, movies)

        for movie_to_remove_user in movies_to_remove_user:
            movie_document = self.es.get(index=movie_index, id=movie_to_remove_user, doc_type='movie')
            users_who_liked_movie = movie_document["_source"]["whoRated"]
            users_who_liked_movie.remove(user_id)
            users_who_liked_movie = list(set(users_who_liked_movie))
            self.es.update(index=movie_index,
                           id=movie_to_remove_user, doc_type='movie',
                           body={"doc": {"whoRated": users_who_liked_movie}})

        for movie_to_add_user in movies_to_add_user:
            movie_document = self.es.get(index=movie_index, id=movie_to_add_user, doc_type='movie')
            users_who_liked_movie = movie_document["_source"]["whoRated"]
            users_who_liked_movie.append(user_id)
            users_who_liked_movie = list(set(users_who_liked_movie))
            self.es.update(index=movie_index,
                           id=movie_to_add_user, doc_type='movie',
                           body={"doc": {"whoRated": users_who_liked_movie}})

        self.es.update(index=user_index, id=user_id,
                       body={"doc": {"ratings": movies}}, doc_type="user")

    def update_movie_document(self, movie_id, users_who_like_movie, user_index="users", movie_index="movies"):
        movie_id = int(movie_id)
        users = list(set(users_who_like_movie))
        to_update = self.es.get(index=movie_index, id=movie_id, doc_type='movie')
        old_users = to_update['_source']['whoRated']

        users_to_add_movie = np.setdiff1d(users, old_users)
        users_to_remove_movie = np.setdiff1d(old_users, users)

        for user_to_remove_movie in users_to_remove_movie:
            user_document = self.es.get(index=user_index, id=user_to_remove_movie, doc_type='user')
            movies_liked_by_user = user_document["_source"]["ratings"]
            movies_liked_by_user.remove(movie_id)
            movies_liked_by_user = list(set(movies_liked_by_user))
            self.es.update(index=user_index,
                           id=user_to_remove_movie, doc_type='user',
                           body={"doc": {"ratings": movies_liked_by_user}})

        for user_to_remove_movie in users_to_add_movie:
            user_document = self.es.get(index=user_index, id=user_to_remove_movie, doc_type='user')
            movies_liked_by_user = user_document["_source"]["ratings"]
            movies_liked_by_user.append(movie_id)
            movies_liked_by_user = list(set(movies_liked_by_user))
            self.es.update(index=user_index,
                           id=user_to_remove_movie, doc_type='user',
                           body={"doc": {"ratings": movies_liked_by_user}})

        self.es.update(index=movie_index, id=movie_id,
                       body={"doc": {"whoRated": users}}, doc_type="movie")

    def delete_user_document(self, user_id, user_index="users", movie_index="movies"):
        user_id = int(user_id)

        to_delete = self.es.get(index=user_index, id=user_id, doc_type='user')["_source"]["ratings"]

        for movie_id_to_update in to_delete:
            movie_document = self.es.get(index=movie_index, id=movie_id_to_update, doc_type='movie')
            users_who_liked_movie = movie_document["_source"]["whoRated"]
            users_who_liked_movie.remove(user_id)
            users_who_liked_movie = list(set(users_who_liked_movie))
            self.es.update(index=movie_index,
                           id=movie_id_to_update, doc_type='movie',
                           body={"doc": {"whoRated": users_who_liked_movie}})

        self.es.delete(index=user_index, id=user_id, doc_type="user")

    def delete_movie_document(self, movie_id, user_index="users", movie_index="movies"):
        movie_id = int(movie_id)
        to_delete = self.es.get(index=movie_index, id=movie_id, doc_type='movie')["_source"]["whoRated"]

        for user_id_to_update in to_delete:
            user_document = self.es.get(index=user_index, id=user_id_to_update, doc_type='user')
            movies_liked_by_user = user_document["_source"]["ratings"]
            movies_liked_by_user.remove(movie_id)
            movies_liked_by_user = list(set(movies_liked_by_user))
            self.es.update(index=user_index,
                           id=user_id_to_update, doc_type='user',
                           body={"doc": {"ratings": movies_liked_by_user}})

        self.es.delete(index=movie_index, id=movie_id, doc_type="movie")

    def bulk_user_update(self, body):
        for item in body:
            self.update_user_document(item["user_id"], item["liked_movies"], user_index='users', movie_index='movies')

    def bulk_movie_update(self, body):
        for item in body:
            self.update_movie_document(item["movie_id"], item["users_who_liked_movie"], user_index='users',
                                       movie_index='movies')


if __name__ == "__main__":
    ec = ElasticClient()
    ec.index_documents()
    # ------ Simple operations ------
    print()
    user_document = ec.get_movies_liked_by_user(75)
    movie_id = np.random.choice(user_document['ratings'])
    movie_document = ec.get_users_that_like_movie(movie_id)
    random_user_id = np.random.choice(movie_document['whoRated'])
    random_user_document = ec.get_movies_liked_by_user(random_user_id)
    print('User 75 likes following movies:')
    print(user_document)
    print('Movie {} is liked by following users:'.format(movie_id))
    print(movie_document)
    print('Is user 75 among users in movie {} document?'.format(movie_id))
    print(movie_document['whoRated'].index(75) != -1)

    some_test_movie_ID = 1
    print("Some test movie ID: ", some_test_movie_ID)
    list_of_users_who_liked_movie_of_given_ID = ec.get_users_that_like_movie(some_test_movie_ID)["whoRated"]
    print("List of users who liked the test movie: ", *list_of_users_who_liked_movie_of_given_ID)
    index_of_random_user_who_liked_movie_of_given_ID = random.randint(0,
                                                                      len(list_of_users_who_liked_movie_of_given_ID))
    print("Index of random user who liked the test movie: ",
          index_of_random_user_who_liked_movie_of_given_ID)
    some_test_user_ID = list_of_users_who_liked_movie_of_given_ID[index_of_random_user_who_liked_movie_of_given_ID]
    print("ID of random user who liked the test movie: ", some_test_user_ID)
    movies_liked_by_user_of_given_ID = ec.get_movies_liked_by_user(some_test_user_ID)["ratings"]
    print("IDs of movies liked by the random user who liked the test movie: ",
          *movies_liked_by_user_of_given_ID)
    if some_test_movie_ID in movies_liked_by_user_of_given_ID:
        print("As expected, the test movie ID is among the IDs of movies " +
              "liked by the random user who liked the test movie ;-)")

    # Preselection
    user_preselection = ec.get_user_preselection(75)
    print("Preselection for user 75")
    print(user_preselection)

    print('Random user {} document who also liked movie {}'.format(random_user_id, movie_id))
    print(random_user_document["ratings"])

    print('Are movies rated by random user {} on recommended movies for user 75?'.format(random_user_id))
    intersect = np.intersect1d(user_preselection, random_user_document["ratings"])
    print(len(intersect) != 0)

    # add user
    new_user_id = 50000
    print('Add user with ID {} that likes movie 3 and 32'.format(new_user_id))
    ec.add_user_document(new_user_id, [3, 32])
    updated_movie_document = ec.get_users_that_like_movie(3)["whoRated"]
    print('After insert user with ID {}, movie 3 was updated {}'.format(new_user_id,
                                                                        new_user_id in updated_movie_document))
    # update user
    print('Updating user with ID {}, now he likes movie 3 and 420'.format(new_user_id))
    ec.update_user_document(new_user_id, [3, 420])
    updated_movie_document = ec.get_users_that_like_movie(420)["whoRated"]
    print("After update, user {} likes movie 420: {}".format(new_user_id, new_user_id in updated_movie_document))
    updated_movie_document = ec.get_users_that_like_movie(32)["whoRated"]
    print("After update, user {} don't like movie 32: {}".format(new_user_id, new_user_id in updated_movie_document))

    # delete user
    print("Delete user with ID {}".format(new_user_id))
    ec.delete_user_document(new_user_id)
    updated_movie_document = ec.get_users_that_like_movie(420)["whoRated"]
    print("After delete of user {} document for movie 420 is updated: {}".format(new_user_id,
                                                                                 new_user_id not in updated_movie_document))
    updated_movie_document = ec.get_users_that_like_movie(3)["whoRated"]
    print("After delete of user {} movie 3 is updated: {}".format(new_user_id, new_user_id not in updated_movie_document))



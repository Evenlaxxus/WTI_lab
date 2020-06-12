import json
from random import random

from flask import Flask, request
import pandas as pd
from models import getData

app = Flask(__name__)
user_genre_rating, second_table, genres = getData()


@app.route('/')
def hello():
    return 'Hello there'


@app.route('/rating', methods=['POST'])
def rating():
    row = request.get_json()
    global user_genre_rating
    user_genre_rating = pd.concat([user_genre_rating, pd.json_normalize(row)])
    print(user_genre_rating)
    return row


@app.route('/ratings', methods=['GET'])
def get_ratings():
    return user_genre_rating.to_json(orient='records')


@app.route('/ratings', methods=['DELETE'])
def delete_ratings():
    request_data = request.get_json()
    indexNames = user_genre_rating[
        (user_genre_rating['movieID'] == request_data['movieID']) & (user_genre_rating['userID']
                                                                     == request_data['userID'])].index
    print(indexNames)
    user_genre_rating.drop(indexNames, inplace=True)
    return 'ok'


@app.route('/avg-genre-rating/all-users', methods=['GET'])
def genre_rating_all_users():
    response = {}
    for column in second_table.columns:
        response[column] = random() * 10
    return response


@app.route('/avg-genre-rating/<userID>', methods=['GET'])
def genre_rating_for_user(userID):
    response = {}
    for genre in genres:
        response[genre] = random() * 10
    response["userID"] = userID
    return response


if __name__ == "__main__":
    app.run()

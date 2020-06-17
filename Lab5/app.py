from flask import Flask, request

from controller import Controller

app = Flask(__name__)

controller = Controller()


@app.route('/')
def hello():
    return 'Hello there'


@app.route('/rating', methods=['POST'])
def rating():
    row = request.get_json()
    return controller.addRow(row), {'Content-Type': 'application/json'}


@app.route('/ratings', methods=['GET'])
def get_ratings():
    return controller.data.fillna(0).to_json(orient="records"), {'Content-Type': 'application/json'}


@app.route('/ratings', methods=['DELETE'])
def delete_ratings():
    request_data = request.get_json()
    controller.deleteRow(request_data)
    return 'ok'


@app.route('/avg-genre-rating/all-users', methods=['GET'])
def genre_rating_all_users():
    return controller.genre_ratings(), {'Content-Type': 'application/json'}


@app.route('/avg-genre-rating/<userID>', methods=['GET'])
def genre_rating_for_user(userID):
    return controller.genre_user_ratings(userID), {'Content-Type': 'application/json'}


if __name__ == '__main__':
    app.run()

import time

import requests
import json


def print_request(r, method='GET'):
    print('METHOD: %s' % method)
    print('request.url: %s' % r.url)
    print('request.status_code: %s' % r.status_code)
    print('request.headers: %s' % r.headers)
    print('request.text: %s' % r.text)
    print('request.request.body: %s' % r.request.body)
    print('request.request.headers: %s' % r.request.headers)
    print('elapsed time: %s' % r.elapsed.total_seconds())


serverUrl = 'http://localhost:5000/'


def client():
    print_request(requests.get(serverUrl + 'ratings'))
    time.sleep(0.01)
    print_request(requests.post(
            serverUrl + 'rating',
            data=json.dumps({
                "Action": 1,
                "Adventure": 1,
                "Animation": 0,
                "Children": 0,
                "Comedy": 0,
                "Crime": 0,
                "Documentary": 0,
                "Drama": 1,
                "Fantasy": 0,
                "Film-Noir": 0,
                "Horror": 0,
                "IMAX": 0,
                "Musical": 0,
                "Mystery": 1,
                "Romance": 1,
                "Sci-Fi": 0,
                "Short": 0,
                "Thriller": 1,
                "War": 0,
                "Western": 0,
                "userID": 1234,
                "movieID": 903,
                "rating": 4.0
            }),
            headers={"Content-Type": "application/json"}),
        'POST')
    time.sleep(0.01)
    print_request(requests.delete(
            serverUrl + 'ratings',
            data=json.dumps({
            "userID": 1234,
            "movieID": 903
        }),
            headers={"Content-Type": "application/json"}),
        'DELETE')
    time.sleep(0.01)
    print_request(requests.get(serverUrl + 'avg-genre-rating/all-users'))
    time.sleep(0.01)
    print_request(requests.get(serverUrl + 'avg-genre-rating/75'))
    time.sleep(0.01)
    print_request(requests.get(serverUrl + 'user-profile/78'))


if __name__ == "__main__":
    client()

import requests as rq
import re

prefix = 'http://127.0.0.1:5000'


def print_response(response, body=None):
    print('Request:')
    print('\tUrl: {}'.format(response.url))
    print('\tMethod: {}'.format(re.search("<PreparedRequest \[(\w+)\]>", str(response.request)).group(1)))
    print('\tBody: {}'.format(body))
    print('Response:')
    print('\tCode: {}'.format(response.status_code))
    content = response.content.decode("utf-8")
    if len(content) > 200:
        content = content[:160] + "..." + content[-38:]
    print('\tContent: {}'.format(content), end='')
    print('\tHeaders: {}'.format(response.headers))
    print('-' * 30)


def send_get(message, url):
    print(message)
    url = prefix + url
    response = rq.get(url)
    print_response(response)


def send_post(message, url, body=None):
    print(message)
    url = prefix + url
    response = rq.post(url, json=body)
    print_response(response, body)


def send_put(message, url, body=None):
    print(message)
    url = prefix + url
    if body is None:
        response = rq.put(url)
    else:
        response = rq.put(url, data=body, headers={"Content-Type": "application/json"})
    print_response(response, body)


def send_delete(message, url):
    print(message)
    url = prefix + url
    response = rq.delete(url)
    print_response(response)


if __name__ == "__main__":
    # Get Test
    send_get('Document for user with ID = 75', '/user/document/75')
    send_get('Non existing user document', '/user/document/0')
    send_get('Document for movie with ID = 3', '/movie/document/3')

    # Preselection Test
    send_get('Preselection for user 75', '/user/preselection/75')
    send_get('Preselection for movie 3', '/movie/preselection/3')

    # Add Test
    send_put('Add new movie document number 80000 that nobody likes', '/movie/document/80000', '[]')
    send_put('Add new movie document number 80001 that nobody likes', '/movie/document/80001', '[]')
    send_put('Add new movie document number 80002 that nobody likes', '/movie/document/80002', '[]')
    send_put('Add new user document number 90000, who likes movies 80000 and 80001', '/user/document/90000', '[80000, 80001]')
    send_get('Get new user 90000 document', '/user/document/90000')
    send_get('Get updated movie 80000 document', '/movie/document/80000')
    send_get('Get updated movie 80001 document', '/movie/document/80001')

    # Update Test
    send_post('Update user 90000, that he now likes movies 80000 and 80002', '/user/bulk', [{"user_id": 90000, "liked_movies": [80000, 80002]}])
    send_get('Get updated user 90000 document', '/user/document/90000')
    send_get('Get updated movie 80000 document', '/movie/document/80000')
    send_get('Get updated movie 80001 document', '/movie/document/80001')
    send_get('Get updated movie 80002 document', '/movie/document/80002')

    # Remove Test
    send_delete('Remove user document number 90000', '/user/document/90000')
    send_get('Get updated movie 80000 document', '/movie/document/80000')
    send_get('Get updated movie 80001 document', '/movie/document/80001')
    send_get('Get updated movie 80002 document', '/movie/document/80002')
    send_delete('Remove movie document number 80000', '/movie/document/80000')
    send_delete('Remove movie document number 80001', '/movie/document/80001')
    send_delete('Remove movie document number 80002', '/movie/document/80002')

    # Index Test
    send_get('List indices', '/index/all')
    send_post('Add index temp', '/index/add')
    send_get('List indices', '/index/all')
    send_post('Reindex users to temp', '/index/reindex')
    send_get('List indices', '/index/all')
    send_get('Get document no 78 from index temp', '/user/document/78?index=temp')
    send_get('Get document no 127 from index temp', '/user/document/127?index=temp')
    send_delete('Remove index temp', '/index/delete')
    send_get('Get all index', '/index/all')
    send_get('List indices', '/index/all')

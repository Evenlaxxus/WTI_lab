from redis import Redis
import time
import json

redis = Redis(host='localhost', port=6379)


def consumer():
    while True:
        elem = redis.lrange("queue", 0, 1)
        redis.ltrim("queue", 1, -1)
        print(elem)
        time.sleep(0.01)


if __name__ == "__main__":
    consumer()

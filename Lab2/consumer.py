from redis import Redis
import time
import json

redis = Redis(host='localhost', port=6379)


def consumer():
    start = time.time()
    while True:
        elem = redis.lrange("queue", 0, 1)
        redis.ltrim("queue", 1, -1)
        print(elem)
        if time.time() - start >= 10:
            break
        time.sleep(0.25)


if __name__ == "__main__":
    consumer()

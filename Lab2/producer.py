from redis import Redis
import time
import json
import pandas as pd

redis = Redis(host='localhost', port=6379)


def producer():
    data = pd.read_table("./user_ratedmovies.dat")

    for row in data.iterrows():
        redis.rpush("queue", row[1].to_json())
        time.sleep(0.01)

    # i = 0
    # while True:
    #     redis.rpush("queue", json.dumps(i))
    #     i += 1
    #     time.sleep(0.01)


if __name__ == "__main__":
    producer()

import matplotlib.pyplot as plt
import pandas as pd

import requests

cherryPyUrl = 'http://localhost:9898/'
flaskUrl = 'http://localhost:9898/'


def client():
    time_frame = pd.DataFrame(columns=[
        'CherryPy',
        'Flask'
    ])

    for i in range(100):
        print("Requests iteration:", i)
        cherry_py_time = requests.get(cherryPyUrl + 'avg-genre-ratings/all-users').elapsed.total_seconds()
        flask_time = requests.get(flaskUrl + 'avg-genre-ratings/all-users').elapsed.total_seconds()
        time_frame = time_frame.append({
            'CherryPy': cherry_py_time,
            'Flask': flask_time,
        }, ignore_index=True)

    print(time_frame)
    plt.xlabel('Requests')
    plt.ylabel('Response Time (sec)')
    plt.title('APIs Response Times')
    plt.ylim(0, 0.4)
    time_frame.plot()
    plt.show()
    plt.savefig('output.png', dpi=300)


if __name__ == "__main__":
    client()

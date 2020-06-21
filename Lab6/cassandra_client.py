from cassandra.cluster import Cluster
from cassandra.query import dict_factory


class Cassandra:
    def __init__(self):
        self.keyspace = "user_ratings"
        self.table = "user_movie_rating"
        cluster = Cluster(['127.0.0.1'], port=9042)
        self.session = cluster.connect()
        self.create_keyspace()
        self.session.set_keyspace(self.keyspace)
        self.session.row_factory = dict_factory
        self.create_table()

    def create_keyspace(self):
        self.session.execute("""
        CREATE KEYSPACE IF NOT EXISTS """ + self.keyspace + """
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
        """)

    def create_table(self):
        self.session.execute("""
        CREATE TABLE IF NOT EXISTS """ + self.keyspace + """.""" + self.table + """(
        userID int,
        movieID int,
        rating float,
        genres map<text, float>,
        PRIMARY KEY(userID, movieID)
        )
        """)

    def insert(self, userID, movieID, rating, genres):
        self.session.execute(
            """
            INSERT INTO """ + self.keyspace + """.""" + self.table + """(
            userID,
            movieID,
            rating,
            genres
            )
            VALUES (%(userID)s, %(movieID)s, %(rating)s, %(genres)s)
            """,
            {
                'userID': userID,
                'movieID': movieID,
                'rating': rating,
                'genres': genres
            }
        )

    def delete(self, userID, movieID):
        self.session.execute(
            "DELETE FROM " + self.keyspace + "." + self.table + " WHERE userID=%(userID)s AND movieID=%(movieID)s",
            {
                'userID': userID,
                'movieID': movieID
            }
        )

    def select(self):
        response = []
        rows = self.session.execute("SELECT * FROM " + self.keyspace + "." + self.table + ";")
        for row in rows:
            dict = {}
            genres = row['genres']
            dict['userID'] = row['userid']
            dict['movieID'] = row['movieid']
            dict['rating'] = row['rating']
            for key in genres:
                dict[key] = genres[key]
            response.append(dict)
        return response

    def clear_table(self):
        self.session.execute("TRUNCATE " + self.keyspace + "." + self.table + ";")

    def drop_table(self):
        self.session.execute("DROP TABLE " + self.keyspace + "." + self.table + ";")

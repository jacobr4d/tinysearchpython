import sys
import argparse
import sqlite3
import time

# for other classes to use database
class Database:
    def __init__(self, database_path):
        self.database_path = database_path

    def tf(self, word, url):
        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute(f"SELECT val FROM tf WHERE word = '{word}' AND url = '{url}'")
        (res,) = cur.fetchone()
        con.close()
        return res

    def idf(self, word):
        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute(f"SELECT val FROM idf WHERE word = '{word}'")
        (res,) = cur.fetchone()
        con.close()
        return res

    def rank(self, url):
        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute(f"SELECT val FROM rank WHERE url = '{url}'")
        (res,) = cur.fetchone()
        con.close()
        return res

    def docs(self, word):
        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute(f"SELECT url FROM tf WHERE word = '{word}'")
        res = [x for (x,) in cur.fetchall()]
        con.close()
        return res

# upload data by running this file
if __name__ == "__main__":
    start_time = time.time()

    # database config is args
    parser = argparse.ArgumentParser(prog="tinysearchpython database", description="stores queryable search data")
    parser.add_argument("--tfs", dest="tfs_path", default="tfs", help="location to get tfs")
    parser.add_argument("--idfs", dest="idfs_path", default="idfs", help="location to get idfs")
    parser.add_argument("--ranks", dest="ranks_path", default="ranks", help="location to get ranks")
    parser.add_argument("--database", dest="database_path", default="database", help="location to put database")
    # parser.add_argument("-v", "--verbose", help="increase output verbosity", action="store_true")
    args = parser.parse_args(sys.argv[1:])

    con = sqlite3.connect(args.database_path)
    cur = con.cursor()
    cur.execute("CREATE TABLE tf(word, url, val REAL)")
    cur.execute("CREATE TABLE idf(word, val REAL)")
    cur.execute("CREATE TABLE rank(url, val REAL)")
    cur.execute('BEGIN TRANSACTION')
    with open(args.tfs_path) as tfs_file:
        for line in tfs_file:
            cur.execute('INSERT INTO tf (word, url, val) VALUES (?,?,?)', line.split())
    with open(args.idfs_path) as idfs_file:
        for line in idfs_file:
            cur.execute('INSERT INTO idf (word, val) VALUES (?,?)', line.split())
    with open(args.ranks_path) as ranks_file:
        for line in ranks_file:
            cur.execute('INSERT INTO rank (url, val) VALUES (?,?)', line.split())
    cur.execute('COMMIT')
    con.close()

    print(f"Database upload time {(time.time() - start_time):.2f}")

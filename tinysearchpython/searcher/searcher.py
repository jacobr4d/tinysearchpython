import sys
import argparse
import sqlite3
from flask import Flask, render_template, request

def tf(word, url):
    con = sqlite3.connect("index.db")
    cur = con.cursor()
    cur.execute(f"SELECT val FROM tf WHERE word = '{word}' AND url = '{url}'")
    (res,) = cur.fetchone()
    con.close()
    return res

def idf(word):
    con = sqlite3.connect("index.db")
    cur = con.cursor()
    cur.execute(f"SELECT val FROM idf WHERE word = '{word}'")
    (res,) = cur.fetchone()
    con.close()
    return res

def rank(url):
    con = sqlite3.connect("index.db")
    cur = con.cursor()
    cur.execute(f"SELECT val FROM rank WHERE url = '{url}'")
    (res,) = cur.fetchone()
    con.close()
    return res

def docs(word):
    con = sqlite3.connect("index.db")
    cur = con.cursor()
    cur.execute(f"SELECT url FROM tf WHERE word = '{word}'")
    res = [x for (x,) in cur.fetchall()]
    con.close()
    return res

# app = Flask(__name__)

# @app.route("/")
# def search():
#     query = request.args.get("query", None)
#     if not query:
#         return render_template('search.html', query=None, results=None)
#     else:


    
#         # return render_template('search.html', results=[1, 2, 3, 4, 5])

# app.run(host='0.0.0.0', port=8000)

if __name__ == "__main__":
    print(tf("dispon", "https://fr.wikipedia.org:443/"))
    print(idf("across"))
    print(rank("https://fr.wikipedia.org:443/"))
    print(docs("word"))
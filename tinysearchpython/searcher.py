import sys
import argparse
import sqlite3
from flask import Flask, render_template, request
import nltk
from nltk.corpus import stopwords
import re

from database import *
from words import stem

parser = argparse.ArgumentParser(prog="tinysearchpython database", description="stores queryable search data")
parser.add_argument("--database", dest="database_path", default="database", help="location to get database")
parser.add_argument("-v", "--verbose", help="increase output verbosity", action="store_true")
args = parser.parse_args(sys.argv[1:])

db = Database(args.database_path)

app = Flask(__name__)

@app.route("/")
def search():
    query = request.args.get("query", None)
    if not query:
        return render_template('search.html', msg="No query")
    words = [stem(x) for x in query.split() if stem(x) != None]
    if not words:
        return render_template('search.html', msg="No terms parsed")
    matching_sets = []
    for word in words:
        matching_set = set(db.docs(word))
        if not matching_set:
            return render_template('search.html', msg=f"Lemnatized word '{word}' has no matches")
        matching_sets.append(matching_set)
    matches = list(set.intersection(*matching_sets))
    if not matches:
        return render_template('search.html', msg="No common matches for all terms")
    results = [{
        "url": x,
        "ir": sum(db.tf(word, x) * db.idf(word) for word in words),
        "pr": db.rank(x),
        "score": db.rank(x) * sum(db.tf(word, x) * db.idf(word) for word in words)
    } for x in matches]
    results.sort(key=lambda x: x["score"], reverse=True)
    return render_template('search.html', results=results, v=args.verbose)

app.run(host='0.0.0.0', port=8000)

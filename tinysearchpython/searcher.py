import sys
import argparse
import sqlite3
from flask import Flask, render_template, request
import nltk
from nltk.corpus import stopwords
import re

from database import *
from words import stem

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
        matching_set = set(docs(word))
        if not matching_set:
            return render_template('search.html', msg=f"Lemnatized word '{word}' has no matches")
        matching_sets.append(matching_set)
    matches = list(set.intersection(*matching_sets))
    if not matches:
        return render_template('search.html', msg="No common matches for all terms")
    results = [{
        "url": x,
        "ir": sum(tf(word, x) * idf(word) for word in words if docs(word)),
        "pr": rank(x),
        "score": rank(x) * sum(tf(word, x) * idf(word) for word in words if docs(word))
    } for x in matches]
    results.sort(key=lambda x: x["score"], reverse=True)
    return render_template('search.html', results=results)

app.run(host='0.0.0.0', port=8000)
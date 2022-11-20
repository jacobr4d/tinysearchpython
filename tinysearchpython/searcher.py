import sys
import logging
import argparse
from flask import Flask, render_template, request
import nltk
from nltk.corpus import stopwords
import re

import asyncio
import aioredis
import words

# endpoints
REDIS_URL = "redis://localhost"        
REDIS_DOCLISTS = "crawler:doclists"
REDIS_TFS = "crawler:tfs"
REDIS_IDFS = "cralwer:idfs"
REDIS_RANKS = "cralwer:ranks"

# parse args
parser = argparse.ArgumentParser(prog="tinysearchpython database", description="stores queryable search data")
parser.add_argument("-q", "--query", required=True, help="search query")
parser.add_argument("-v", "--verbose", help="increased output verbosity", action="store_true")
parser.add_argument("-d", "--debug", help="max output verbosity", action="store_true")
args = parser.parse_args(sys.argv[1:])
if args.debug:
    logging.basicConfig(level=logging.DEBUG)
elif args.verbose:
    logging.basicConfig(level=logging.INFO)

# asynchronous redis connection
aredis = aioredis.from_url(REDIS_URL).client()

if not args.query:
    sys.exit(0)
terms = args.query.split()
logging.debug(f"terms: {terms}")
if not terms:
    sys.exit(0)
lemns = [words.stem(term) for term in terms]
logging.debug(f"lemns: {lemns}")

async def get_doclist_sizes():
    return await asyncio.gather(*[aredis.scard(f"{REDIS_DOCLISTS}:{lemn}") for lemn in lemns])

doclist_sizes = asyncio.get_event_loop().run_until_complete(get_doclist_sizes())
logging.debug(f"doclist_sizes: {doclist_sizes}")
if any([not card for card in doclist_sizes]):
    sys.exit(0)

lemn_with_smallest_doclist = lemns[doclist_sizes.index(min(doclist_sizes))]
logging.debug(f"lemn_with_smallest_doclist: {lemn_with_smallest_doclist}")

async def get_idfs():
    return await asyncio.gather(*[aredis.get(f"{REDIS_IDFS}:{lemn}") for lemn in lemns])
idfs = asyncio.get_event_loop().run_until_complete(get_idfs())
logging.debug(f"idfs: {idfs}")

# async def get_data_for_url(doc):
#     tfs = [aredis.]
#     jobs = [aredis.scard(f"{REDIS_DOCLISTS}:{lemn}") for lemn in lemns]
#     results = await asyncio.gather(*jobs)
#     return results


# search strategy
# split query into terms
# look at doclist count for each term
# let term* be term with smallest doclist count
# doc in term* doclist:
#   get tf(term, doc) for all term in query
#   get rank(doc)
#   compute score(doc)
#   update max score window

# OPTIMIZATION: get doclist sorted by pagerank

# app = Flask(__name__)

# @app.route("/")
# def search():
#     query = request.args.get("query", None)
#     if not query:
#         return render_template('search.html', msg="No query")
#     words = [stem(x) for x in query.split() if stem(x) != None]
#     if not words:
#         return render_template('search.html', msg="No terms parsed")
#     matching_sets = []
#     for word in words:
#         matching_set = set(db.docs(word))
#         if not matching_set:
#             return render_template('search.html', msg=f"Lemnatized word '{word}' has no matches")
#         matching_sets.append(matching_set)
#     matches = list(set.intersection(*matching_sets))
#     if not matches:
#         return render_template('search.html', msg="No common matches for all terms")
#     results = [{
#         "url": x,
#         "ir": sum(db.tf(word, x) * db.idf(word) for word in words),
#         "pr": db.rank(x),
#         "score": db.rank(x) * sum(db.tf(word, x) * db.idf(word) for word in words)
#     } for x in matches]
#     results.sort(key=lambda x: x["score"], reverse=True)
#     return render_template('search.html', results=results, v=args.verbose)

# app.run(host='0.0.0.0', port=8000)

asyncio.run(aredis.close())

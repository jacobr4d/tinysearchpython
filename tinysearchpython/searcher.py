import sys
import logging
import argparse
from flask import Flask, render_template, request
import nltk
from nltk.corpus import stopwords
import re
import uvloop

import asyncio
import aioredis
import words

# endpoints
REDIS_URL = "redis://localhost"        
REDIS_DOCLISTS = "crawler:doclists"
REDIS_TFS = "crawler:tfs"
REDIS_IDFS = "crawler:idfs"
REDIS_RANKS = "crawler:ranks"

# parse args
parser = argparse.ArgumentParser(prog="tinysearchpython database", description="stores queryable search data")
parser.add_argument("-n", "--number", type=int, default=10, help="number of results")
# parser.add_argument("-q", "--query", required=True, help="search query")
parser.add_argument("-v", "--verbose", help="increased output verbosity", action="store_true")
parser.add_argument("-d", "--debug", help="max output verbosity", action="store_true")
args = parser.parse_args(sys.argv[1:])
if args.debug:
    logging.basicConfig(level=logging.DEBUG)
elif args.verbose:
    logging.basicConfig(level=logging.INFO)

# asynchronous redis connection
aredis = aioredis.from_url(REDIS_URL).client()

# OPTIMIZATION: get doclist sorted by pagerank

app = Flask(__name__)

@app.route("/")
def search():

    loop = uvloop.new_event_loop()
    
    query = request.args.get("query", None)
    if not query:
        return render_template('search.html', msg="No query")
    terms = query.split()
    logging.debug(f"terms: {terms}")
    if not terms:
        return render_template('search.html', msg="No query terms")
    lemns = [words.stem(term) for term in terms]
    logging.debug(f"lemns: {lemns}")

    async def get_doclist_sizes():
        return await asyncio.gather(*[aredis.scard(f"{REDIS_DOCLISTS}:{lemn}") for lemn in lemns])

    doclist_sizes = loop.run_until_complete(get_doclist_sizes())
    logging.debug(f"doclist_sizes: {doclist_sizes}")
    if any([not card for card in doclist_sizes]):
        return render_template('search.html', msg="Some term has no matches")

    lemn_with_smallest_doclist = lemns[doclist_sizes.index(min(doclist_sizes))]
    logging.debug(f"lemn_with_smallest_doclist: {lemn_with_smallest_doclist}")

    async def get_idfs():
        return await asyncio.gather(*[aredis.get(f"{REDIS_IDFS}:{lemn}") for lemn in lemns])

    idfs = loop.run_until_complete(get_idfs())
    idfs = [float(x) for x in idfs]
    logging.debug(f"idfs: {idfs}")

    async def get_score(url):
        tfs = await asyncio.gather(*[aredis.get(f"{REDIS_TFS}:{lemn}:{url}") for lemn in lemns])
        rank = await aredis.get(f"{REDIS_RANKS}:{url}")
        tfs = [float(tf) if tf else 0 for tf in tfs]
        rank = float(rank) if rank else 0
        return rank * sum(tfs[i] * idfs[i] for i in range(len(lemns)))

    results = []
    async def do_work(results):
        async for burl in aredis.sscan_iter(f"{REDIS_DOCLISTS}:{lemn_with_smallest_doclist}"):
            url = burl.decode()
            score = await get_score(url)
            results.append({"url": url, "score": score})
            results.sort(reverse=True, key=lambda x: x["score"])
            if len(results) > args.number:
                results = results[:args.number]
    loop.run_until_complete(do_work(results))
    return render_template('search.html', results=results, msg="Locally optimal results", v=(args.verbose or args.debug))

app.run(host='0.0.0.0', port=8080)

import sys
import logging
import argparse
import redis
from flask import Flask, render_template, request
import nltk
from nltk.corpus import stopwords
import re
import uvloop
import ujson

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

# synchronous redis connection
sredis = redis.Redis() 


async def get_doclist_sizes():
    return await asyncio.gather(*[aredis.scard(f"{REDIS_DOCLISTS}:{lemn}") for lemn in lemns])

async def get_idfs():
    return await asyncio.gather(*[aredis.get(f"{REDIS_IDFS}:{lemn}") for lemn in lemns])

async def get_score(url):
    tfs = await asyncio.gather(*[aredis.get(f"{REDIS_TFS}:{lemn}:{url}") for lemn in lemns])
    rank = await aredis.get(f"{REDIS_RANKS}:{url}")
    tfs = [float(tf) if tf else 0 for tf in tfs]
    rank = float(rank) if rank else 0
    return rank * sum(tfs[i] * idfs[i] for i in range(len(lemns)))

async def get_multiple_scores(urls):
    scores = await asyncio.gather(*[get_score(url) for url in urls])
    return [(scores[i], urls[i]) for i in range(len(urls))]

# OPTIMIZATION: get doclist sorted by pagerank
try:
    terms = args.query.split()
    logging.debug(f"terms: {terms}")
    if not terms:
        print("no query terms")
        sys.exit(0)

    lemns = [words.stem(term) for term in terms if words.stem(term)]
    logging.debug(f"lemns: {lemns}")
    if not lemns:
        print("no lemns (stopwords are removed)")
        sys.exit(0) 

    doclist_sizes = asyncio.get_event_loop().run_until_complete(get_doclist_sizes())
    logging.debug(f"doclist_sizes: {doclist_sizes}")
    if not doclist_sizes or any([not card for card in doclist_sizes]):
        print("some term has no matches")
        sys.exit(0)

    # get idfs once
    idfs = asyncio.get_event_loop().run_until_complete(get_idfs())
    idfs = [float(x) for x in idfs]
    logging.debug(f"idfs: {idfs}")

    # intersection strategy
    urls = sredis.sinter([f"{REDIS_DOCLISTS}:{lemn}" for lemn in lemns])
    urls = [url.decode() for url in urls]
    logging.debug(f"intersection size: {len(urls)}")
    results = asyncio.get_event_loop().run_until_complete(get_multiple_scores(list(urls)))
    results.sort(reverse=True, key=lambda x: x[0])
    if len(results) > args.number:
        results = results[:args.number]
    for result in results:
        print(f"{result[0]} {result[1]}")


    # union strategy

    # lemn_with_smallest_doclist = lemns[doclist_sizes.index(min(doclist_sizes))]
    # logging.debug(f"lemn_with_smallest_doclist: {lemn_with_smallest_doclist}")

    # results = []
    # async def do_work(results):
    #     async for burl in aredis.sscan_iter(f"{REDIS_DOCLISTS}:{lemn_with_smallest_doclist}"):
    #         url = burl.decode()
    #         score = await get_score(url)
    #         results.append({"url": url, "score": score})
    #         results.sort(reverse=True, key=lambda x: x["score"])
    #         if len(results) > args.number:
    #             results = results[:args.number]
    # asyncio.get_event_loop().run_until_complete(do_work(results))
    # print(ujson.dumps(results, escape_forward_slashes=False, indent=2))
finally:
    asyncio.get_event_loop().run_until_complete(aredis.close())
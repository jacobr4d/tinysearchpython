import sys
import argparse
import asyncio
import aioredis
import aiofiles
from aiohttp import ClientSession, ClientTimeout, ClientError
import aiohttp
import lxml.html
from typing import ByteString
from robots import *
import redis
from url import *
import logging
import time
from words import stem
import atexit
import ssl
import uvloop
import signal

# endpoints
REDIS_URL = "redis://localhost"        
REDIS_NUM_CRAWLED = "crawler:num_crawled"     # VAR in DEFAULT MAP
REDIS_FRONTIER = "crawler:frontier"           # SET
REDIS_SEEN_URLS = "crawler:seen_urls"         # SET
REDIS_ROBOTS = "crawler:robots"               # DEFAULT MAP (host:str -> rules:json-str)
REDIS_ACCESSES = "crawler:domain_accesses"    # DEFAULT MAP (host:str -> time:str)

# args to configure crawler
parser = argparse.ArgumentParser(prog="tinysearchpython crawl", description="crawls the web")
parser.add_argument("--seeds", dest="seeds_path", default="seeds", help="location to get seed urls")
parser.add_argument("-c", "--concurrency", dest="concurrency", type=int, default=32, help="number of pages to process concurrently")
parser.add_argument("--urls", dest="urls_path", default="data/urls", help="file to store crawled urls")
parser.add_argument("--hits", dest="hits_path", default="data/hits", help="file to store hits")
parser.add_argument("--links", dest="links_path", default="data/links", help="file to store links")
parser.add_argument("--size", dest="page_size_limit_bytes", type=int, default=1000000, help="page size limit bytes (not retrieved if over)")
parser.add_argument("-v", "--verbose", help="increased output verbosity", action="store_true")
parser.add_argument("-d", "--debug", help="max output verbosity", action="store_true")
args = parser.parse_args(sys.argv[1:])
if args.debug:
    logging.basicConfig(level=logging.DEBUG)
elif args.verbose:
    logging.basicConfig(level=logging.INFO)

# set event loop to uvloop, supposed to be fast
loop = uvloop.new_event_loop()
asyncio.set_event_loop(loop)

# synchronous redis connection
sredis = redis.Redis()  

# asynchronous redis connection
aredis = aioredis.from_url(REDIS_URL).client()

# init some variables for the crawl
sredis.sadd(REDIS_FRONTIER, *[str(Url(x.strip())) for x in open(args.seeds_path).readlines()])
sredis.set(REDIS_NUM_CRAWLED, "0")

# file pointers (keep number limited, or beware)
urls_file = open(args.urls_path, "w")
links_file = open(args.links_path, "w")
hits_file = open(args.hits_path, "w")

running = True
async def loop():
    new_urls = []
    async with ClientSession(connector=aiohttp.TCPConnector(force_close=True)) as session:
        while running:
            # get url
            burl = await aredis.spop(REDIS_FRONTIER)
            if not burl:
                await asyncio.sleep(1)
                continue
            url = Url(burl.decode())
            logging.debug(f"crawling {url}")
            # robots tests
            brules = await aredis.get(f"{REDIS_ROBOTS}:{url.host}")
            if not brules:
                logging.debug(f"no rules yet for {url.host}")
                rules = Rules("tinypythoncrawler", 0)
                await rules.fetch_from(session, False, str(url))
                logging.debug(rules.serialize())
                await aredis.set(f"{REDIS_ROBOTS}:{url.host}", rules.serialize())
            else:
                rules = Rules("tinypythoncrawler", 0)
                rules.deserialize_from(brules.decode())
            if rules.disallows(url):
                logging.debug(f"disallowed {str(url)}")
                continue
            if rules.delay > 0:
                baccessed = await aredis.get(f"{REDIS_ACCESSES}:{url.host}")
                if await aredis.get(f"{REDIS_ACCESSES}:{url.host}") != None:
                    if time.time() - float(baccessed.decode()) < rules.delay:
                        logging.debug(f"delayed {str(url)}")
                        new_urls.append(str(url))
                        continue
                await aredis.set(f"{REDIS_ACCESSES}:{url.host}", time.time())
            # head tests 
            # TO:DO allow pages with no content-length 
            try:
                async with session.head(str(url), ssl=False, timeout=10) as response:
                    logging.debug(f"sending head {str(url)}")
                    if response.status != 200:
                        logging.debug(f"filtered: non 200 head {str(url)}")
                        continue
                    if 'content-type' not in response.headers:
                        logging.debug(f"filtered: no content-type {str(url)}")
                        continue
                    if not response.headers['content-type'].startswith("text/html"):
                        logging.debug(f"filtered: non-html content-type {str(url)}")
                        continue
                    # if 'content-length' not in response.headers:
                    #     logging.debug(f"filtered: no content-length {str(url)}")
                    #     continue
                    # if int(response.headers['content-length']) > args.page_size_limit_bytes:
                    #     logging.debug(f"filtered: big content-length {response.headers['content-length']} {url}")
                    #     continue
            except Exception as e:
                logging.info(f"head exception: {e} {str(url)}")
                continue
            # get
            try:
                async with session.get(str(url), ssl=False, timeout=10) as response:
                    logging.debug(f"sending get {str(url)}")
                    if response.status != 200:
                        logging.debug(f"filtered: non 200 get {str(url)}")
                        continue
                    # record url
                    urls_file.write(str(url) + "\n")
                    logging.info(await aredis.incr(REDIS_NUM_CRAWLED))
                    # parse links
                    page = await response.text()
                    doc = lxml.html.fromstring(page)
                    doc.make_links_absolute(str(url), resolve_base_href=True)
                    links = set()
                    for _, _, link, _ in doc.iterlinks():
                        try:
                            new_url = Url(link)
                        except Exception as e:
                            logging.debug(f"filtered: malformed url {link[:20]}...")
                            continue
                        # record link
                        links.add(str(new_url))
                        new_urls.append(str(new_url))
                    # record links
                    for link in links:
                        links_file.write(f"{str(url)} {link}\n")
                    # add new urls to frontier
                    new_urls = list(set(new_urls))
                    if new_urls:
                        res = sredis.smismember(REDIS_SEEN_URLS, *new_urls)
                        urls_to_add = [new_urls[i] for i in range(len(res)) if not res[i]]
                        if urls_to_add:
                            sredis.sadd(REDIS_SEEN_URLS, *urls_to_add)
                            await aredis.sadd(REDIS_FRONTIER, *urls_to_add)
                    new_urls = []
                    # record hits
                    for word in page.split():
                        lemn = stem(word)
                        if lemn:
                            hits_file.write(f"{lemn} {str(url)}\n")
            except Exception as e:
                logging.info(f"get exception: {e} {str(url)}")
                continue

async def main():
    jobs = [loop() for _ in range(args.concurrency)]
    await asyncio.gather(*jobs)

def signal_handler(signal, frame):
    global running
    logging.info("shutting down")
    running = False

signal.signal(signal.SIGINT, signal_handler)
asyncio.run(main())
asyncio.run(aredis.close())
urls_file.close()
links_file.close()
hits_file.close()
logging.info("done")
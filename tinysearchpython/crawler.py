import sys
import re
import atexit
import argparse
import logging
import time
from threading import Thread
import uuid


import requests
import lxml.html

from url import * 
from robot import *
from words import stem

def crawl(args, thread_id):
    with open(f"{args.urls_path}/{uuid.uuid1()}", "w") as urls_file:
        with open(f"{args.hits_path}/{uuid.uuid1()}", "w") as hits_file:
            with open(f"{args.links_path}/{uuid.uuid1()}", "w") as links_file:

                new_urls = []
                while (True):
                    try:
                        url = Url(session.post(f"{args.frontier}/pop", timeout=10).json()["url"])
                    except Exception as e:
                        logging.error(f"Exception in pop: {e}")
                        continue

                    # ROBOTS.TXT check
                    try:
                        message = session.post(f"{args.frontier}/robot", json={"url": str(url)}, timeout=10).json()["message"]
                    except Exception as e:
                        logging.error(f"Exception in robot: {e}")
                        continue
                    if message == "disallowed":
                        logging.info(f"filtered: disallowed {url}")
                        continue
                    if message == "delayed":
                        logging.info(f"delayed: {url}")
                        new_urls.append(str(url))
                        continue
                        
                    # HEAD check
                    try:
                        head = requests.head(url, headers={"user-agent": "tinysearchpython", "connection": "close"}, timeout=1, allow_redirects=True)
                    except Exception as e:
                        logging.info(f"filtered: head failed: {e}")
                        continue
                    if head.status_code != requests.codes.ok:
                        logging.info(f"filtered: head status code {head.status_code} {url}")
                        head.close()
                        continue
                    if 'content-type' not in head.headers:
                        logging.info(f"filtered: no content-type {url}")
                        head.close()
                        continue
                    if not head.headers['content-type'].startswith("text/html"):
                        logging.info(f"filtered: non-html content-type {url}")
                        head.close()
                        continue
                    if 'content-length' not in head.headers:
                        logging.info(f"filtered: no content-length {url}")
                        head.close()
                        continue
                    if int(head.headers['content-length']) > args.page_size_limit_bytes:
                        logging.info(f"filtered: bad content-length {head.headers['content-length']} {url}")
                        head.close()
                        continue
                    head.close()
                    
                    # GET
                    try:
                        get = requests.get(url, headers={"user-agent": "tinysearchpython", "connection": "close"}, timeout=1, allow_redirects=True)
                    except Exception as e:
                        logging.info(f"filtered: get failed: {e}")
                        continue
                    if get.status_code != requests.codes.ok:
                        logging.info(f"filtered: get status code {get.status_code} {url}")
                        get.close()
                        continue
                    get.close()

                    print(str(url), file=urls_file, flush=True)

                    # LINKS
                    doc = lxml.html.fromstring(get.text)
                    doc.make_links_absolute(str(url), resolve_base_href=True)

                    for _, _, link, _ in doc.iterlinks():
                        try:
                            new_url = Url(link)
                        except Exception as e:
                            logging.info(f"filtered: malformed url {link}")
                            continue
                        new_urls.append(str(new_url))
                        print(str(url), str(new_url), file=links_file, flush=True)

                    # CHECK if URLS are SEEN
                    try:
                        seen_bits = session.post(f"{args.frontier}/seen", json={"urls": new_urls}, timeout=10).json()["seen"]
                    except Exception as e:
                        logging.error(f"exception in seen: {e}")
                        continue
                    urls_to_add = [new_urls[i] for i in range(len(seen_bits)) if seen_bits[i] == 0]
                    # give queue new urls
                    try:
                        session.post(f"{args.frontier}/push", json={"urls": urls_to_add}, timeout=10)
                    except Exception as e:
                        logging.error(f"exception in push: {e}")
                        continue
                    new_urls = []

                    # for word in get.text.split():
                    #     word = stem(word)
                    #     if word == None:
                    #         continue
                    #     print(word, url, file=hits_file, flush=True)
                    hits_file.writelines([f"{stem(word)} {str(url)}\n" for word in get.text.split() if stem(word)])
                    hits_file.flush()


# crawler config is args
parser = argparse.ArgumentParser(prog="tinysearchpython crawl", description="crawls the web")
parser.add_argument("--threads", type=int, default=1, help="number of threads")
parser.add_argument("--frontier", default="http://localhost:9000", help="location of frontier")
parser.add_argument("--urls", dest="urls_path", default="urls", help="dir to store crawled urls")
parser.add_argument("--hits", dest="hits_path", default="hits", help="dir to store hits")
parser.add_argument("--links", dest="links_path", default="links", help="location to store links")
# parser.add_argument("--count", dest="count_path", default="count", help="location to final count of crawled docs")
# parser.add_argument("--log", dest="log_path", default="logs/logs", help="where to put log")
parser.add_argument("--bpp", dest="page_size_limit_bytes", type=int, default=1000000, help="page size limit bytes (not processed if over)")
parser.add_argument("-v", "--verbose", help="increase output verbosity", action="store_true")
args = parser.parse_args(sys.argv[1:])
if args.verbose:
    logging.basicConfig(level=logging.INFO)

urls_log = logging.StreamHandler()

session = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=args.threads, pool_maxsize=args.threads)
session.mount('http://', adapter)
session.mount('https://', adapter)

for i in range(args.threads):
    t1 = Thread(target=crawl, args=(args, i))
    t1.start()
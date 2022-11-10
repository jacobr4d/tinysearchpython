import sys
import re
import atexit
import argparse
import logging
import time
from threading import Thread

import requests
import lxml.html

from url import * 
from robot import *
from words import stem



# # write count for further processing
# def write_count():
#     with open(args.count_path, "w") as count_file:
#         print(count, file=count_file)

# # dump log at end
# def log():
#     print("SUMMARY")
#     print(f"robots map size {len(robots)}")
#     print(f"downloaded count {count}")

# atexit.register(write_count)
# atexit.register(log)

def crawl(args, thread_id):
    with open(f"{args.urls_path}_{thread_id}", "w") as urls_file, open(f"{args.hits_path}_{thread_id}", "w") as hits_file, open(f"{args.links_path}_{thread_id}", "w") as links_file, open(f"{args.log_path}_{thread_id}", "w") as log_file:
        count = 0
        new_urls = []
        while (True):
            url = Url(requests.post(f"{args.frontier}/pop", timeout=3).json()["url"])

            # ROBOTS.TXT check
            message = requests.post(f"{args.frontier}/robot", json={"url": str(url)}, timeout=7).json()["message"]
            if message == "disallowed":
                logging.info(f"filtered: disallowed {url}")
                continue
            if message == "delayed":
                logging.info(f"delayed: {url}")
                new_urls.append(str(url))
                continue
                
            # HEAD check
            try:
                head = requests.head(url, headers={"user-agent": "tinysearchpython"}, timeout=3, allow_redirects=True)
            except Exception as e:
                print("Exception in head:", e, flush=True)
                continue
            if head.status_code != requests.codes.ok:
                logging.info(f"filtered: head status code {head.status_code} {url}")
                continue
            if 'content-type' not in head.headers:
                logging.info(f"filtered: no content-type {url}")
                continue
            if not head.headers['content-type'].startswith("text/html"):
                logging.info(f"filtered: non-html content-type {url}")
                continue
            if 'content-length' not in head.headers:
                logging.info(f"filtered: no content-length {url}")
                continue
            if int(head.headers['content-length']) > args.page_size_limit_bytes:
                logging.info(f"filtered: bad content-length {head.headers['content-length']} {url}")
                continue
            
            # GET
            try:
                get = requests.get(url, headers={"user-agent": "tinysearchpython"}, timeout=3, allow_redirects=True)
            except Exception as e:
                print("Exception in get:", e, flush=True)
                continue
            if get.status_code != requests.codes.ok:
                logging.info(f"filtered: get status code {get.status_code} {url}")
                continue
            logging.info(f"{count} downloaded: {url}")
            count += 1
            print(time.time(), count, file=log_file, flush=True)

            # # parse links
            doc = lxml.html.fromstring(get.text)
            doc.make_links_absolute(str(url), resolve_base_href=True)

            for _, _, link, _ in doc.iterlinks():
                try:
                    new_url = Url(link)
                except Exception as e:
                    logging.info(f"filtered: malformed url {link}")
                    continue
                new_urls.append(str(new_url))
            
            # check if urls seen
            seen_bits = requests.post(f"{args.frontier}/seen", json={"urls": new_urls}, timeout=7).json()["seen"]
            urls_to_add = [new_urls[i] for i in range(len(seen_bits)) if seen_bits[i] == 0]
            # give queue new urls
            requests.post(f"{args.frontier}/push", json={"urls": urls_to_add}, timeout=7)
            new_urls = []

            # hits_parsed = 0
            # for word in get.text.split():
            #     if hits_parsed > args.hits_per_page_limit:
            #         break
            #     word = stem(word)
            #     if word == None:
            #         continue
            #     print(word, url, file=hits_file)
            #     hits_parsed += 1

if __name__ == "__main__":
    # crawler config is args
    parser = argparse.ArgumentParser(prog="tinysearchpython crawl", description="crawls the web")
    parser.add_argument("--threads", type=int, default=1, help="number of threads")
    parser.add_argument("--frontier", default="http://localhost:9000", help="location of frontier")
    parser.add_argument("--urls", dest="urls_path", default="urls/urls", help="location to store crawled urls")
    parser.add_argument("--hits", dest="hits_path", default="hits/hits", help="location to store hits")
    parser.add_argument("--links", dest="links_path", default="links/links", help="location to store links")
    # parser.add_argument("--count", dest="count_path", default="count", help="location to final count of crawled docs")
    parser.add_argument("--log", dest="log_path", default="logs/logs", help="where to put log")
    parser.add_argument("--bpp", dest="page_size_limit_bytes", type=int, default=1000000, help="page size limit bytes (not processed if over)")
    parser.add_argument("-v", "--verbose", help="increase output verbosity", action="store_true")
    argz = parser.parse_args(sys.argv[1:])
    if argz.verbose:
        logging.basicConfig(level=logging.INFO)

    for i in range(argz.threads):
        t1 = Thread(target=crawl, args=(argz, i))
        t1.start()

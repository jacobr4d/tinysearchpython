import sys
import re
import atexit
import argparse
import logging

import requests
from lxml.html import iterlinks, make_links_absolute, fromstring
import nltk
from nltk.corpus import stopwords

from url import * 
from robot import *

# static structs to import
ps = nltk.stem.PorterStemmer()
stop_words_set = set(stopwords.words("english"))

# crawler config is args
parser = argparse.ArgumentParser(prog="tinysearchpython crawl", description="crawls the web")
parser.add_argument("--seeds", dest="seeds_path", default="seeds", help="location to get seed urls")
parser.add_argument("--urls", dest="urls_path", default="urls", help="location to store crawled urls")
parser.add_argument("--hits", dest="hits_path", default="hits", help="location to store hits")
parser.add_argument("--links", dest="links_path", default="links", help="location to store links")
parser.add_argument("--count", dest="count_path", default="count", help="location to final count of crawled docs")
parser.add_argument("--bpp", dest="page_size_limit_bytes", type=int, default=1000000, help="page size limit bytes (not processed if over)")
parser.add_argument("--hpp", dest="hits_per_page_limit", type=int, default=1000000, help="hits per page limit (subset processed if over)")
parser.add_argument("--lpp", dest="links_per_page_limit", type=int, default=1000000, help="links per page limit (subset processed if over)")
parser.add_argument("-v", "--verbose", help="increase output verbosity", action="store_true")
args = parser.parse_args(sys.argv[1:])
if args.verbose:
    logging.basicConfig(level=logging.INFO)

# crawl state
frontier = [Url(x.strip()) for x in open(args.seeds_path).readlines()]
seen = set(str(x) for x in frontier)
robots = {}
count = 0

# write count for further processing
def write_count():
    with open(args.count_path, "w") as count_file:
        print(count, file=count_file)

# dump log at end
def log():
    print("SUMMARY")
    print("CONFIG")
    print(f"seeds path {args.seeds_path}")
    print(f"hits path {args.hits_path}")
    print(f"links path {args.links_path}")
    print(f"page size limit bytes {args.page_size_limit_bytes}")
    print(f"hits per page limit {args.hits_per_page_limit}")
    print(f"links per page limit {args.links_per_page_limit}")
    print("CRAWL STATE")
    print(f"frontier list size {len(frontier)}")
    print(f"seen set size {len(seen)}")
    print(f"robots map size {len(robots)}")
    print(f"downloaded count {count}")

atexit.register(write_count)
atexit.register(log)

# crawl
with open(args.urls_path, "w") as urls_file, open(args.hits_path, "w") as hits_file, open(args.links_path, "w") as links_file:
    while (True):
        url = frontier.pop(0)

        # ROBOTS.TXT check
        if url.host not in robots:
            robots[url.host] = Robot(url)
        robot = robots[url.host]
        if robot.disallows(url):
            logging.info(f"filtered: disallowed {url}")
            continue
        if robot.delays():
            logging.info(f"delayed: {robot.last_accessed} {robot.delay} {url}")
            frontier.append(url)
            continue
            
        # HEAD check
        head = requests.head(url, headers={"user-agent": "tinysearchpython"}, timeout=3, allow_redirects=True)
        if head.status_code != requests.codes.ok:
            logging.info(f"filtered: head status code {head.status_code} {url}")
            continue
        if 'content-type' not in head.headers:
            logging.info(f"filtered: no content-type {url}")
            continue
        if not head.headers['content-type'].startswith("text/html"):
            logging.info(f"filtered: bad content-type {url}")
            continue
        if 'content-length' not in head.headers:
            logging.info(f"filtered: no content-length {url}")
            continue
        if int(head.headers['content-length']) > args.page_size_limit_bytes:
            logging.info(f"filtered: bad content-length {head.headers['content-length']} {url}")
            continue
        
        # GET
        get = requests.get(url, headers={"user-agent": "tinysearchpython"}, timeout=3, allow_redirects=True)
        if get.status_code != requests.codes.ok:
            logging.info(f"filtered: get status code {get.status_code} {url}")
            continue
        robot.update_last_accessed()
        logging.info(f"{count} downloaded: {url}")
        count += 1

        # parse links
        html = fromstring(get.text)
        html.make_links_absolute(str(url))
        links_parsed = 0
        # give every page a link to itself, so that it is aknowledged in pagerank
        print(url, file=urls_file)
        for _, _, link, _ in html.iterlinks():
            if links_parsed > args.links_per_page_limit:
                break
            try:
                new_url = Url(link)
            except Exception as e:
                logging.info(f"filtered: malformed url {link}")
                continue
            else:
                print(url, new_url, file=links_file)
                links_parsed += 1
                if not str(new_url) in seen:
                    frontier.append(new_url)
                    seen.add(str(new_url))

        # parse hits
        # steps:
        # replace common punctuation (,.) with spaces
        # filter is word (full match [a-zA-Z']+)
        # regularize case (lowercase)
        # filter is not stop word     
        # apply stemmer           
        text = get.text.replace(",", " ")
        text = text.replace(".", " ")
        links_parsed = 0
        for word in get.text.split():
            if links_parsed > args.links_per_page_limit:
                break
            if not re.fullmatch("[a-zA-Z']{2,}", word):
                continue
            word = word.lower()
            if word in stop_words_set:
                continue
            word = ps.stem(word)
            print(word, url, file=hits_file)
            links_parsed += 1
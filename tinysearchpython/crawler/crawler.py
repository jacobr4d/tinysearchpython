import sys
import re
import atexit
import argparse

import requests
from lxml.html import iterlinks, make_links_absolute, fromstring
import nltk
from nltk.corpus import stopwords

from url import * 
from robot import *

# static structs to import
ps = nltk.stem.PorterStemmer()
stop_words_set = set(stopwords.words("english"))

# parse arguments used to config crawling
parser = argparse.ArgumentParser(prog="tinysearchpython", description="crawl, index, pagerank, search engine")
parser.add_argument("--hits", default="hits", help="location to store hits")
parser.add_argument("--links", default="links", help="location to store links")
parser.add_argument("--bpp", default="1000000", help="page size limit bytes (not processed if over)")
parser.add_argument("--hpp", default="1000000", help="hits per page limit (subset processed if over)")
parser.add_argument("--lpp", default="1000000", help="links per page limit (subset processed if over)")
args = parser.parse_args(sys.argv[1:])

hits_path = args.hits
links_path = args.links
page_size_limit_bytes = int(args.bpp)
hits_per_page_limit = int(args.hpp)
links_per_page_limit = int(args.lpp)

# init crawl
count = 0
robots = {}
frontier = [Url("https://en.wikipedia.org/wiki/Homo_antecessor")]
seen = set(str(x) for x in frontier)                                  # set of url strings
def log():
    print(f"hits path {hits_path}")
    print(f"links path {links_path}")
    print(f"page size limit bytes {page_size_limit_bytes}")
    print(f"hits per page limit {hits_per_page_limit}")
    print(f"links per page limit {links_per_page_limit}")
    print(f"frontier list size {len(frontier)}")
    print(f"seen set size {len(seen)}")
atexit.register(log)


# crawl
with open(hits_path, "w") as hits_file, open(links_path, "w") as links_file:
    while (True):
        url = frontier.pop(0)

        # ROBOTS.TXT check
        if url.host not in robots:
            robots[url.host] = Robot(url)
        robot = robots[url.host]
        if robot.disallows(url):
            print(f"filtered: disallowed {url}")
            continue
        if robot.delays():
            print(f"delayed: {url}")
            frontier.append(url)
            continue
            
        # HEAD check
        head = requests.head(url, headers={"user-agent": "tinysearchpython"}, timeout=3)
        if head.status_code != requests.codes.ok:
            print(f"filtered: head status code {head.status_code} {url}")
            continue
        if 'content-type' not in head.headers:
            print(f"filtered: no content-type {url}")
            continue
        if not head.headers['content-type'].startswith("text/html"):
            print(f"filtered: bad content-type {url}")
            continue
        if 'content-length' not in head.headers:
            print(f"filtered: no content-length {url}")
            continue
        if int(head.headers['content-length']) > page_size_limit_bytes:
            print(f"filtered: bad content-length {head.headers['content-length']} {url}")
            continue
        
        # GET
        get = requests.get(url, headers={"user-agent": "tinysearchpython"}, timeout=3)
        if get.status_code != requests.codes.ok:
            print(f"filtered: get status code {get.status_code} {url}")
            continue
        
        print(f"{count} downloaded: {url}")
        count += 1

        # parse links
        html = fromstring(get.text)
        html.make_links_absolute(str(url))
        links_parsed = 0
        for _, _, link, _ in html.iterlinks():
            if links_parsed > links_per_page_limit:
                break
            try:
                new_url = Url(link)
            except Exception as e:
                print(f"filtered: malformed url {link}")
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
            if links_parsed > links_per_page_limit:
                break
            if not re.fullmatch("[a-zA-Z']+", word):
                continue
            word = word.lower()
            if word in stop_words_set:
                continue
            word = ps.stem(word)
            print(word, url, file=hits_file)
            links_parsed += 1
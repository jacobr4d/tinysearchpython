import sys
import re

import requests
from lxml.html import iterlinks, make_links_absolute, fromstring
import nltk
from nltk.corpus import stopwords

from url import * 
from robot import *

hits_path = "hits"
links_path = "links"
page_size_limit_bytes = 1000000     # 1 MB
hits_per_page_limit = 100
links_per_page_limit = 100
ps = nltk.stem.PorterStemmer()
stop_words_set = set(stopwords.words("english"))

class Crawler:

    def __init__(self):
        self.count = 0
        self.robots = {}
        self.frontier = [Url("https://en.wikipedia.org/wiki/Homo_antecessor")]
        self.seen = set(str(x) for x in self.frontier)                                  # set of url strings

    def crawl(self):
        with open(hits_path, "w") as hits_file, open(links_path, "w") as links_file:
            while (True):
                url = self.frontier.pop(0)

                # ROBOTS.TXT check
                if url.host not in self.robots:
                    self.robots[url.host] = Robot(url)
                robot = self.robots[url.host]
                if robot.disallows(url):
                    print(f"filtered: disallowed {url}")
                    continue
                if robot.delays():
                    print(f"delayed: {url}")
                    self.frontier.append(url)
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
                
                print(f"{self.count} downloaded: {url}")
                self.count += 1

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
                        if not str(new_url) in self.seen:
                            self.frontier.append(new_url)

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


if __name__ == '__main__':
    crawler = Crawler()
    crawler.crawl()

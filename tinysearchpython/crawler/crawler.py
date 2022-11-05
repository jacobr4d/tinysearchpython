import sys

import requests
from lxml.html import iterlinks, make_links_absolute, fromstring

from url import * 
from robot import *

hitsPath = "hits"
linksPath = "links"
sizeLimit = 1000000     # 1 MB

class Crawler:

    def __init__(self):
        self.frontier = [Url("https://en.wikipedia.org/wiki/Homo_antecessor")]
        self.robots = {}
        self.count = 0
        self.seen = set(str(x) for x in self.frontier)                                  # set of url strings

    def crawl(self):
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
                print(f"filtered: head status code {head.status_code}")
                continue
            if 'content-type' not in head.headers:
                print("filtered: no content-type")
                continue
            if not head.headers['content-type'].startswith("text/html"):
                print("filtered: bad content-type")
                continue
            if 'content-length' not in head.headers:
                print("filtered: no content-length")
                continue
            if int(head.headers['content-length']) > sizeLimit:
                print(f"filtered: bad content-length {head.headers['content-length']}")
                continue
            
            # GET
            get = requests.get(url, headers={"user-agent": "tinysearchpython"}, timeout=3)
            if get.status_code != requests.codes.ok:
                print(f"filtered: get status code {get.status_code}")
                continue
            
            print(f"{self.count} downloaded: {url}")
            self.count += 1

            # parse links
            html = fromstring(get.text)
            html.make_links_absolute(str(url))
            for _, _, link, _ in html.iterlinks():
                try:
                    new_url = Url(link)
                    if not str(new_url) in self.seen:
                        self.frontier.append(new_url)
                except Exception as e:
                    print(f"filtered: malformed url {new_url}")


if __name__ == '__main__':
    crawler = Crawler()
    crawler.crawl()

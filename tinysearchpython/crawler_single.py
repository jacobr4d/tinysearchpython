import sys
import re
import atexit
import argparse
import logging
import time
from threading import Thread
import uuid
from persistqueue import SQLiteQueue, PDict
import copy
from multiprocessing import Process, Lock


import requests
import lxml.html

from url import * 
from robot import *
from words import stem

def send_threads(args, robots_lock):
    for _ in range(args.threads):
        t = Thread(target=crawl, args=(args,robots_lock))
        t.start()

def crawl(args, robots_lock):
    frontier = SQLiteQueue("frontier", auto_commit=True, multithreading=True)
    seen_urls = PDict("seen_urls", "seen_urls_name", multithreading=True)
    robots = PDict("robots", "robots_name", multithreading=True)

    with open(f"{args.urls_path}/{uuid.uuid1()}", "w") as urls_file:
        with open(f"{args.hits_path}/{uuid.uuid1()}", "w") as hits_file:
            with open(f"{args.links_path}/{uuid.uuid1()}", "w") as links_file:

                new_urls = []
                while (True):

                    url = Url(frontier.get())

                    # ROBOTS.TXT check
                    robot_url = copy.copy(url)
                    robot_url.path = "/robots.txt"
        
                    robots_lock.acquire()
                    robot = None
                    if str(robot_url) in robots:
                        robot = robot_from_string(robots[str(robot_url)])
                    else:
                        robot = robot_from_robot_url(str(robot_url))
                        robots[str(robot_url)] = str(robot)

                    if robot.disallows(url):
                        logging.info(f"filtered: disallowed {url}")
                        robots_lock.release()
                        continue
                    if robot.delays():
                        logging.info(f"delayed: {url}")
                        new_urls.append(str(url))
                        robots_lock.release()
                        continue
                    else:
                        robot.update_last_accessed()
                        robots[str(robot_url)] = str(robot)
                    robots_lock.release()

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
                        print(str(url), str(new_url), file=links_file, flush=True)
                        new_urls.append(str(new_url))

                    # CHECK if URLS are SEEN
                    # seen_urls_lock.acquire()
                    for x in new_urls:
                        if x not in seen_urls:
                            seen_urls[x] = "P"
                            frontier.put(x)
                    # seen_urls_lock.release()
                    new_urls = []

                    hits_file.writelines([f"{stem(word)} {str(url)}\n" for word in get.text.split() if stem(word)])
                    hits_file.flush()

if __name__ == "__main__":

    # crawler config is args
    parser = argparse.ArgumentParser(prog="tinysearchpython crawl", description="crawls the web")
    parser.add_argument("--seeds", dest="seeds_path", default="seeds", help="location to get seed urls")
    parser.add_argument("-p", "--procs", type=int, default=1, help="number of procs")
    parser.add_argument("-t", "--threads", type=int, default=1, help="number of threads")
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

    frontier = SQLiteQueue("frontier", auto_commit=True, multithreading=True)
    for url in [str(Url(x.strip())) for x in open(args.seeds_path).readlines()]:
        frontier.put(url)
    seen_urls = PDict("seen_urls", "seen_urls_name", multithreading=True)               # using perisisten dict, but only using key SET functionality :O
    for url in [str(Url(x.strip())) for x in open(args.seeds_path).readlines()]:
        seen_urls[url] = "P"
    robots = PDict("robots", "robots_name", multithreading=True)

    robots_lock = Lock() # the only strictly necessary lock

    procs = []


    for _ in range(args.procs):
        p = Process(target=send_threads, args=(args,robots_lock))
        p.start()
        procs.append(p)

    for proc in procs:
        proc.join()
import sys
import argparse
from flask import Flask, render_template, request
import logging
from threading import Lock
from persistqueue import SQLiteQueue, PDict

from url import * 
from robot import *

parser = argparse.ArgumentParser(prog="frontier", description="frontier for crawler")
# parser.add_argument("--threads", type=int, default=1, help="number of threads to keep connections with")
parser.add_argument("--port", default="9000", help="port to listen on")
parser.add_argument("--seeds", dest="seeds_path", default="seeds", help="location to get seed urls")
parser.add_argument("-v", "--verbose", help="increase output verbosity", action="store_true")
args = parser.parse_args(sys.argv[1:])
if args.verbose:
    logging.basicConfig(level=logging.INFO)

app = Flask(__name__)

# frontier state (a list of string)
frontier_lock = Lock()
frontier = SQLiteQueue("frontier", auto_commit=True, multithreading=True)
for url in [str(Url(x.strip())) for x in open(args.seeds_path).readlines()]:
    frontier.put(url)

seen_urls_lock = Lock()
seen_urls = PDict("seen_urls", "seen_urls_name", multithreading=True)       # using perisisten dict, but only using key SET functionality :O
for url in [str(Url(x.strip())) for x in open(args.seeds_path).readlines()]:
    seen_urls[url] = "P"

robots_lock = Lock()
robots = PDict("robots", "robots_name", multithreading=True)

# accepts list of urls as JSON :)
@app.route("/push", methods=["POST"])
def push():
    frontier_lock.acquire()
    if not request.is_json:
        raise Exception("request is not json!")
    for url in request.get_json()["urls"]:
        frontier.put(url)
    # frontier.extend(request.get_json()["urls"])
    frontier_lock.release()
    return ""

# reuturns head of queue as JSON
@app.route("/pop", methods=["POST"])
def pop():
    frontier_lock.acquire()
    logging.info(f"frontier {len(frontier)} seen {len(seen_urls)}")
    ret = {"url": frontier.get()}
    frontier_lock.release()
    return ret

# return bitmask for seen already, mark all as seen hereafter
@app.route("/seen", methods=["POST"])
def seen():
    seen_urls_lock.acquire()
    urls = request.get_json()["urls"]
    ret_list = []
    for x in urls:
        ret_list.append(1 if x in seen_urls else 0)
        seen_urls[x] = "P"
    ret = {"seen": ret_list}
    # seen_urls.update(urls)
    seen_urls_lock.release()
    return ret

@app.route("/robot", methods=["POST"])
def robot():
    robots_lock.acquire()
    url = request.get_json()["url"]
    url_obj = Url(url)
    url_obj.path = "/robots.txt"
    robot = None
    if str(url_obj) in robots:
        robot = robot_from_string(robots[str(url_obj)])
    else:
        robot = robot_from_robot_url(str(url_obj))
        robots[str(url_obj)] = str(robot)
    ret = None
    if robot.disallows(url_obj):
        ret = {"message": "disallowed"}
    if robot.delays():
        ret = {"message": "delayed"}
    else:
        robot.update_last_accessed()
        robots[str(url_obj)] = str(robot)
        ret = {"message": "allowed"}
    robots_lock.release()
    return ret


app.run(host='0.0.0.0', port=args.port)

import sys
import argparse
from flask import Flask, render_template, request
import logging

from url import * 
from robot import *

parser = argparse.ArgumentParser(prog="frontier", description="frontier for crawler")
parser.add_argument("--port", default="9000", help="port to listen on")
parser.add_argument("--seeds", dest="seeds_path", default="seeds", help="location to get seed urls")
parser.add_argument("-v", "--verbose", help="increase output verbosity", action="store_true")
args = parser.parse_args(sys.argv[1:])
if args.verbose:
    logging.basicConfig(level=logging.INFO)

app = Flask(__name__)

# frontier state (a list of string)
frontier = [str(Url(x.strip())) for x in open(args.seeds_path).readlines()]
seen_urls = set(x for x in frontier)
robots = {}

# accepts list of urls as JSON :)
@app.route("/push", methods=["POST"])
def push():
    if not request.is_json:
        raise Exception("request is not json!")
    frontier.extend(request.get_json()["urls"])
    return ""

# reuturns head of queue as JSON
@app.route("/pop", methods=["POST"])
def pop():
    logging.info(f"frontier {len(frontier)} seen {len(seen_urls)}")
    return {"url": frontier.pop(0)}

# return bitmask for seen already, mark all as seen hereafter
@app.route("/seen", methods=["POST"])
def seen():
    urls = request.get_json()["urls"]
    ret = {"seen": [1 if x in seen_urls else 0 for x in urls]}
    seen_urls.update(urls)
    return ret

@app.route("/robot", methods=["POST"])
def robot():
    url = request.get_json()["url"]
    url_obj = Url(url)
    if url_obj.host not in robots:
        robots[url_obj.host] = Robot(url_obj)
    robot = robots[url_obj.host]
    if robot.disallows(url_obj):
        return {"message": "disallowed"}
    if robot.delays():
        return {"message": "delayed"}
    else:
        robot.update_last_accessed()
        return {"message": "allowed"}


app.run(host='0.0.0.0', port=args.port)

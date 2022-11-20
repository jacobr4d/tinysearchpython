import sys
import argparse
import time
import fileinput

"""
Generate redis proto for our database upload
Usage: python upload_data.py | redis-cli --pipe
"""

parser = argparse.ArgumentParser(prog="tinysearchpython upload data", description="stores queryable search data to redis")
parser.add_argument("--tfs", dest="tfs_path", default="data/tfs", help="location to get tfs")
parser.add_argument("--idfs", dest="idfs_path", default="data/idfs", help="location to get idfs")
parser.add_argument("--ranks", dest="ranks_path", default="data/ranks", help="location to get ranks")
args = parser.parse_args(sys.argv[1:])

def redis_proto(redis_cmd):
    args = redis_cmd.split()
    proto = ""
    proto += "*" + str(len(args)) + "\r\n"
    for arg in args:
        proto += "$" + str(len(arg)) + "\r\n" + arg + "\r\n"
    return proto

with open(args.tfs_path) as tfs_file:
    for line in tfs_file:
        word, url, tf = line.split()
        cmd = f"SADD crawler:doclists:{word} {url}"
        proto = redis_proto(cmd)
        sys.stdout.write(proto)

with open(args.tfs_path) as tfs_file:
    for line in tfs_file:
        word, url, tf = line.split()
        cmd = f"SET crawler:tfs:{word}:{url} {tf}" 
        proto = redis_proto(cmd)
        sys.stdout.write(proto)

with open(args.idfs_path) as idfs_file:
    for line in idfs_file:
        word, idf = line.split()
        cmd = f"SET crawler:idfs:{word} {idf}" 
        proto = redis_proto(cmd)
        sys.stdout.write(proto)

with open(args.ranks_path) as ranks_file:
    for line in ranks_file:
        url, rank = line.split()
        cmd = f"SET crawler:ranks:{url} {rank}" 
        proto = redis_proto(cmd)
        sys.stdout.write(proto)
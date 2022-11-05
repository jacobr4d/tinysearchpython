import sys
import argparse
import math
from tempfile import NamedTemporaryFile
from fileinput import input
from glob import glob

from pyspark import SparkContext, SparkConf

# indexer config is args
parser = argparse.ArgumentParser(prog="tinysearchpython indexer", description="indexes from crawl data")
parser.add_argument("--hits", dest="hits_path", default="hits", help="location to get hits")
parser.add_argument("--tfs", dest="tfs_path", default="tfs", help="location to put tfs")

# parser.add_argument("--tfs", dest="tfs_path", default="tfs", help="location to put tfs")

parser.add_argument("-v", "--verbose", help="increase output verbosity", action="store_true")
args = parser.parse_args(sys.argv[1:])
if args.verbose:
    logging.basicConfig(level=logging.INFO)

#setup
appName="tinysearchpython_indexer"
master="local"
conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)

# def printrdd(rdd):
#   for row in rdd.collect():
#     print(row)

# lines are word <space> url
hits = sc.textFile(args.hits_path)
thing = hits.map(lambda x: (x, 1))
sums = thing.reduceByKey(lambda x, y: x + y)
tfs = sums.map(lambda x: (x[0], math.log(1 + x[1])))
formatted = tfs.map(lambda tup: " ".join([str(x) for x in tup]))


tempFile = NamedTemporaryFile(delete=True)
tempFile.close()
formatted.saveAsTextFile(tempFile.name)
with open(args.tfs_path, "w") as tfs_file:
    for line in sorted(input(glob(tempFile.name + "/part-0000*"))):
        tfs_file.write(line)

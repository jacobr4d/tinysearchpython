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
parser.add_argument("--links", dest="links_path", default="links", help="location to get links")
parser.add_argument("--count", dest="count_path", default="count", help="location to get count of crawled docs")
parser.add_argument("--tfs", dest="tfs_path", default="tfs", help="location to put tfs")
parser.add_argument("--idfs", dest="idfs_path", default="idfs", help="location to put idfs")
parser.add_argument("--ranks", dest="ranks_path", default="ranks", help="location to put ranks")
# parser.add_argument("-v", "--verbose", help="increase output verbosity", action="store_true")
args = parser.parse_args(sys.argv[1:])
# if args.verbose:
#     logging.basicConfig(level=logging.INFO)

# PySpark Setup
appName="tinysearchpython_indexer"
master="local"
conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)

# Helper function to save RDD to file
def save_rdd(rdd, file_name):
    tempFile = NamedTemporaryFile(delete=True)
    tempFile.close()
    rdd.saveAsTextFile(tempFile.name)
    with open(file_name, "w") as file:
        for line in sorted(input(glob(tempFile.name + "/part-0000*"))):
            file.write(line)

# Batch compute tfs
# lines are word <space> url
hits = sc.textFile(args.hits_path)
thing = hits.map(lambda x: (x, 1))
sums = thing.reduceByKey(lambda x, y: x + y)
tfs = sums.map(lambda x: (x[0], math.log(1 + x[1])))
formatted = tfs.map(lambda tup: " ".join([str(x) for x in tup]))
save_rdd(formatted, args.tfs_path)

# Batch compute idfs
# lines are word <space> url <space> tf
count = int([x.strip() for x in open(args.count_path).readlines()][0]) 
tfs= sc.textFile(args.tfs_path)
thing = tfs.map(lambda x: (x.split(' ')[0], 1))
sums = thing.reduceByKey(lambda x, y: x + y)
idfs = sums.map(lambda x: (x[0], math.log(count / (1 + x[1]) + 1)))
formatted = idfs.map(lambda tup: " ".join([str(x) for x in tup]))
save_rdd(formatted, args.idfs_path)

# Batch compute prs
# lines are fromURL <space> toURL, read them and remove duplicates
lines = sc.textFile(args.links_path)
lines = lines.distinct()

# all links
fromTo = lines.map(lambda x: (x.split(' ')[0], x.split(' ')[1]))
toFrom = fromTo.map(lambda x: (x[1], x[0]))
outList = fromTo.groupByKey()
inList = toFrom.groupByKey()
outDegree = fromTo.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x + y)
inDegree = toFrom.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x + y)

# sets of urls
urls = fromTo.flatMap(lambda x: [x[0], x[1]]).distinct()
n = urls.count()
froms = fromTo.map(lambda x: x[0]).distinct()
tos = fromTo.map(lambda x: x[1]).distinct()
sinks = urls.subtract(froms)

# print("urls, froms, tos, sinks")
# print(urls.count(), froms.count(), tos.count(), sinks.count())

# init (url, rank)
l = 0.8
rank = urls.map(lambda x: (x, 1.0 / n)) 

for i in range(5):
  inRankNonSinks = outList.join(rank).flatMap(lambda x: [(i, float (x[1][1])/len(x[1][0])) for i in x[1][0]]).reduceByKey(lambda x, y: x + y)
  inRankAll = rank.leftOuterJoin(inRankNonSinks).map(lambda x: (x[0], x[1][1] if x[1][1] != None else 0))
  sinkRank = sinks.map(lambda x: (x, "dummy")).join(rank).map(lambda x: x[1][1]).reduce(lambda x, y: x + y)
  rank = inRankAll.map(lambda x: (x[0], (1 - l + l * sinkRank / n + l * x[1])))

rank = rank.map(lambda line: " ".join([str(x) for x in line]))
save_rdd(rank, args.ranks_path)
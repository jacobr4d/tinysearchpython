import sys
import argparse
import math
import time
from tempfile import NamedTemporaryFile
from fileinput import input
from glob import glob
import logging

from pyspark import SparkContext, SparkConf

# indexer config is args
parser = argparse.ArgumentParser(prog="tinysearchpython indexer", description="indexes from crawl data")
parser.add_argument("--urls", dest="urls_path", default="data/urls", help="file / dir to get crawled urls")
parser.add_argument("--hits", dest="hits_path", default="data/hits", help="file / dir to get hits")
parser.add_argument("--links", dest="links_path", default="data/links", help="file / dir to get links")
parser.add_argument("--tfs", dest="tfs_path", default="data/tfs", help="location to put tfs")
parser.add_argument("--idfs", dest="idfs_path", default="data/idfs", help="location to put idfs")
parser.add_argument("--ranks", dest="ranks_path", default="data/ranks", help="location to put ranks")
parser.add_argument("-v", "--verbose", help="increase output verbosity", action="store_true")
args = parser.parse_args(sys.argv[1:])
if args.verbose:
    logging.basicConfig(level=logging.INFO)

# PySpark Setup
appName="tinysearchpython_indexer"
master="local"
conf = SparkConf().setAppName(appName).setMaster(master)
spark = SparkContext(conf=conf)

# Helper function to save RDD to file
def save_rdd(rdd, file_name):
    tempFile = NamedTemporaryFile(delete=True)
    tempFile.close()
    rdd.saveAsTextFile(tempFile.name)
    with open(file_name, "w") as file:
        for line in sorted(input(glob(tempFile.name + "/part*"))):
            file.write(line)

start_time = time.time()

# Compute TFS
logging.info("producing tfs")
tfs = (
    spark.textFile(args.hits_path)              # word<space>url\n 
    .map(lambda x: (x, 1))                      # (word<space>url, 1)
    .reduceByKey(lambda x, y: x + y)            # (word<space>url, count)
    .map(lambda x: (x[0], math.log(1 + x[1])))  # (word<space>url, tf)
)
save_rdd(tfs.map(lambda x: f"{x[0]} {x[1]}"), args.tfs_path)

crawled_urls = spark.textFile(args.urls_path)
crawled_urls_count = crawled_urls.count()

# Compute IDFS
logging.info("producing idfs")
idfs = (
    tfs                                                                     # (word<space>url, tf)
    .map(lambda x: (x[0].split(" ")[0], 1))                                 # (word, 1)
    .reduceByKey(lambda x, y: x + y)                                        # (word, df)
    .map(lambda x: (x[0], math.log(crawled_urls_count / (1 + x[1] + 1))))   # (word, idf)      
)
save_rdd(idfs.map(lambda x: f"{x[0]} {x[1]}"), args.idfs_path)

# Compute RANKS 
logging.info("producing ranks")
crawled_urls_set_broadcast = spark.broadcast(set(crawled_urls.collect()))
links = (
    spark.textFile(args.links_path)     # url1<space>url2\n
    .map(lambda x: (x.split(" ")[0], x.split(" ")[1]))
    .filter(lambda x: x[1] in crawled_urls_set_broadcast.value)
)

sinks = crawled_urls.subtract(links.map(lambda x: x[0]))
in_ranks_zero = crawled_urls.map(lambda x: (x, 0))
lam = 0.8


cur_ranks = crawled_urls.map(lambda x: (x, 1.0 / crawled_urls_count))

for i in range(5):
    total_sink_rank = (
        sinks.map(lambda x: (x, "dummy"))                  # (sink, "dummy")...
        .join(cur_ranks)                                   # (sink, ("dummy", sink_rank))
        .map(lambda x: x[1][1])                            # (sink_rank)...
        .reduce(lambda x, y: x + y)                        # total_sink_rank
    )
    in_ranks_tos = (
        links                                                                       # (from, to)
        .groupByKey()                                                               # (from, [tos...])
        .join(cur_ranks)                                                            # (from, ([tos...], from_rank))
        .flatMap(lambda x: [(y, float (x[1][1])/len(x[1][0])) for y in x[1][0]])    # (to, single_in_rank)
        .reduceByKey(lambda x, y: x + y)                                            # (to, total_in_rank)
    )
    cur_ranks = (
        in_ranks_zero                               
        .union(in_ranks_tos)                            
        .reduceByKey(lambda x, y: x + y)                # (url, total_in_rank_or_zero)
        .map(lambda x: (x[0], (1 - lam + lam * total_sink_rank / crawled_urls_count + lam * x[1])))          # (url, rank)
    )

save_rdd(cur_ranks.map(lambda x: f"{x[0]} {x[1]}"), args.ranks_path)
print(f"Execution time {time.time() - start_time}")
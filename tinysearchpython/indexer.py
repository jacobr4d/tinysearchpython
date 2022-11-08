import sys
import argparse
import math
from tempfile import NamedTemporaryFile
from fileinput import input
from glob import glob

from pyspark import SparkContext, SparkConf

# indexer config is args
parser = argparse.ArgumentParser(prog="tinysearchpython indexer", description="indexes from crawl data")
parser.add_argument("--urls", dest="urls_path", default="urls", help="location to get crawled urls")
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
        for line in sorted(input(glob(tempFile.name + "/part*"))):
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


# Batch compute page ranks
# lines are fromURL <space> toURL, read them and remove duplicates
crawled_urls = sc.textFile(args.urls_path).distinct()
print(f"{crawled_urls.count()} crawled urls")
print(crawled_urls.take(5))

links = sc.textFile(args.links_path).distinct()
print(f"{links.count()} unique links")
print(links.take(5))

from_to = links.map(lambda x: (x.split(' ')[0], x.split(' ')[1]))
froms = from_to.map(lambda x: x[0]).distinct()
tos = from_to.map(lambda x: x[1]).distinct()
urls = crawled_urls.union(froms).union(tos).distinct()
print(f"{urls.count()} known urls (crawled, from, or to)")
print(urls.take(5))

# total rank must stay the same, therefore we need to 
# distribute tank articifically from the "sinks",
# ie the urls that aren't "froms"
sinks = urls.subtract(froms)

# rank computed for every known url
n = urls.count()
lparam = 0.8
rank = urls.map(lambda x: (x, 1.0 / n)) 

for i in range(5):
    in_rank_tos = (
        from_to.groupByKey()                                                                # (from, [tos...])...
        .join(rank)                                                                         # (from, ([tos...], rank))... 
        .flatMap(lambda x: [(y, float (x[1][1])/len(x[1][0])) for y in x[1][0]])            # (to, rank_from_from)...
        .reduceByKey(lambda x, y: x + y)                                                    # (to, total_rank_from_froms)...
    )
    in_rank_all = (
        rank.leftOuterJoin(in_rank_tos)                                                     # (url, (rank, in_rank_to | None))
        .map(lambda x: (x[0], x[1][1] if x[1][1] != None else 0))                           # (url, in_rank_to | 0)
    )
    total_sink_rank = (
        sinks.map(lambda x: (x, "dummy"))                                                   # (sink, "dummy")...
        .join(rank)                                                                         # (sink, ("dummy", sink_rank))
        .map(lambda x: x[1][1])                                                             # (sink_rank)...
        .reduce(lambda x, y: x + y)                                                         # total_sink_rank
    )
    rank = (
        in_rank_all                                                                         # (url, in_rank)
        .map(lambda x: (x[0], (1 - lparam + lparam * total_sink_rank / n + lparam * x[1]))) # (url, rank)
    )

formatted_rank = rank.map(lambda line: " ".join([str(x) for x in line]))
save_rdd(formatted_rank, args.ranks_path)
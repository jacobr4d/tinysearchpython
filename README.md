A small and understandable HTML page crawler, indexer, and search engine

~ 370 lines (sloccount)

## Demo
```bash
git clone https://github.com/jacobr4d/tinysearchpython.git
cd tinysearchpython
python tinysearchpython/crawler.py -v
python tinysearchpython/indexer.py
python tinysearchpython/database.py
python tinysearchpython/searcher.py -v
# navigate to localhost:8000
```


# Design & Thought Process
## Goals
### Qualitative
- Batch crawling (not aiming for continuous crawling like google)
- Crawl web in BFS fashion starting from seed URLS
- Support multiple search terms
### Quantitative
- Capacity to crawl, index, search over 1M pages
- Crawls, indexes 1M pages in < 1 day
- Return search results in seconds

## High Level Strategy
- **crawl** pages and generate crawl data
- **index** pages (batch compute over crawl data to generate search data)
- **store** search data in database for fast queries
- **serve** search queries through web interface using database to provide search results quickly

<p align="center">
  <img src="https://raw.githubusercontent.com/jacobr4d/tinysearchpython/master/docs/design.png">
</p>

## Requirements Estimates
### Storage
If we want 1M pages, asssuming 1K words per page, we will have 1B hits:
<p align="center">
1M pages * 1K words/page = 1B hits
</p>
Assuming each hit can be store in 100 bytes, storing all hits will take 100 GB of storage:
<p align="center">
1B * 100 bytes = 100 GB
</p>
Assuming 500 links per page, we will store 100M links total:
<p align="center">
1M pages * 500 links/page = 500M links
</p>
Assuming each link can be stored in 100 bytes, storing the links will take 50 GB of storeage:
<p align="center">
500M links * 100 bytes/link = 50GB
</p>
Therefore the crawl data will be 150 GB in total:
<p align="center">
100GB + 50GB = 150 GB
</p>
Assuming that the search data will be smaller than the crawl data, we will need another 150 GB for search data. Therefore we will need 300 GB total for the file system, but since we don't want to get close to the limit, we should get a file stystem with a capacity for 400GB. Also, our database must store 150 GB of data, so its capacity should be > 200 GB.

### Speed
If we want to crawl 1M pages in 12 hrs, then we need to generate crawl data for a page at a rate of:
<p align="center">
1M pages/12hrs = ~ 23 pages / second
</p>
If we want to index 1M pages in 12 hrs, then we need to generate search data for a page at the same rate.


### Summmary
- Our crawler needs to download and process > 23 pages / second
- Our indexer needs to process > 23 pages / second
- Our file system needs capacity > 400 GB
- Our database capacity needs to be > 200 GB
- Our estimate for crawl data will be about 150GB (150 KB / page)
- Our estimate for search data will be about 150GB (150 KB / page)

# First Iteration
## Detailed Design
- Crawler is simple, in that
  - all crawler state is in memory (url_frontier, urls_seen, ...)
  - single cycle (one thread download and processes one page at a time)
- Indexing happens on one machine and it's file system (as opposed to distributed) using Spark
- Database is SQLite, and on one machine
  - should result in fast queries
  - has big enough capacity for our needs
- Search is simple, in that
  - tfs, idfs, and pagerank are combined to produce ranking of results
  - for every query term, data from all the pages with that term are fetched from index (not scalable with respect to many query terms)
  - all these fetched results are put in python lists
<p align="center">
  <img src="https://raw.githubusercontent.com/jacobr4d/tinysearchpython/master/docs/iteration_1.png">
</p>

## Quantitative Evalutation
We did a test crawl of 1000 pages from the seed "https://wikipedia.org" 
### Results
- Space
  - Crawl data is ~87 MB (87 KB / page)
  - Search data is ~32 MB (32 KB / page)
  - Database size is ~32 MB (32 KB / page)
- Time
  - The crawler crawled 1000 pages in ~ 1 pages / second
  - The indexer indexed 1000 pages in 89 seconds giving an average speed of 11 pages / second
  - Database uploaded 1000 pages in 0.7 seconds giving an average speed of 1,400 pages / second
  - With a corpus of 1000 pages, search for uncommon words are < 1 second, but search for "wikipedia" is 10 seconds

### Conclusions
- Space
  - Our estimate of space requirements was a great conservative estimate
- Time
  - crawler needs to be > 10x faster
  - indexer needs to be > 2x faster
  - database upload is good
  - search needs to be redesigned to be more scalable

# Second Iteration
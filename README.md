A small and understandable HTML page crawler, indexer, and search engine

Scales to at least 50k pages

~ 516 lines (sloccount)

## Demo
```bash
git clone https://github.com/jacobr4d/tinysearchpython.git
cd tinysearchpython
# run redis-server somewhere
python tinysearchpython/crawler.py -v --seeds seeds
# press ^C to stop crawler
python tinysearchpython/indexer.py
python tinysearchpython/upload_data.py | redis-cli --pipe
python tinysearchpython/searcher.py -v -q "some search terms"
```

# Design
## Goals
- Batch crawling, indexing, etc. (not aiming for continuous crawling like google)
- Crawl web from seed URLS, don't download same page twice
- Cache robots.txt and respect robots.txt crawl delay directive for user-agent: *
- Scale to 1,000,000 pages, and be able to crawl, index, etc. in < 1 day
- Return results for any query in seconds
- Incorperate TF, IDF, and page rank into search results

## High Level Strategy
- **crawl** pages and generate crawl data
- **index** pages (batch compute over crawl data to generate search data)
- **database** stores search data and provides fast queries for the searcher
- **searcher** provides search results

<p align="center">
  <img src="https://raw.githubusercontent.com/jacobr4d/tinysearchpython/master/docs/design.png">
</p>

## Requirements Estimates
### Analysis
#### Space
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

#### Time
If we want to crawl 1M pages in 12 hrs, then we need to generate crawl data for a page at a rate of:
<p align="center">
1M pages/12hrs = ~ 23 pages / second
</p>
If we want to index 1M pages in 12 hrs, then we need to generate search data for a page at the same rate.

### Conclusion
- Space
  - Our estimate for crawl data will be about 150GB (150 KB / page)
  - Our estimate for search data will be about 150GB (150 KB / page)
  - Therefore our file system needs capacity > 400 GB
  - Therefore our database needs capacity > 200 GB
- Time
  - Crawler needs to download and process > 23 pages / second
  - Indexer needs to process > 23 pages / second

# First Iteration
commit 991a2de631b331e30a1b3a6515e0d52d85f09503
## Detailed Design
- Crawler
  - all crawler state is in memory (url_frontier, urls_seen, ...)
  - synchronous and sequential (get page url from frontier, download page, processes page, repeat)
- Indexing
  - PySpark RDDs
- Database 
  - SQLite
  - should result in fast queries
  - has big enough capacity for our needs
- Search
  - data for pages for terms is unioned 
  - all these fetched results are put in python lists
<p align="center">
  <img src="https://raw.githubusercontent.com/jacobr4d/tinysearchpython/master/docs/iteration_1.png">
</p>

## Quantitative Evalutation
We did a test crawl of 1000 pages from the seed "https://wikipedia.org" 
### Results

| Step | Stats | Stats Comparable |
| --- | --- | --- |
| Crawler crawl | 17 Minutes | 1 pages / second 🐢 | 
| Indexer index | 89 seconds | 11 pages / second |
| Database upload | 0.7 seconds | 1,400 pages / second |
| Search (queries with most results) | 10+ seconds | slow |
| Total data (crawl + search) | 119 MB | 119 KB / page |
| Total time | 1 Hour 16 Minutes | 10 pages / second |

### Conclusions
- Our estimate of space requirements was right on
- Crawler needs to be at least __10x faster__ 🐢
- Indexer needs to be at least __2x faster__ 🐢
- Search needs to scale better 📈 (queries with a lot of results need to return faster)

# Second Iteration

## Detailed Design

## Quantitative Evaluation

Test crawl of 50,000 HTML pages

### Results

| Part | Stats | Stats Comparable |
| --- | --- | --- |
| Crawler | 1 Hour 3 Minutes | 16 pages / second | 
| Indexer | 11 Minutes | 75 pages / second |
| Database (upload) | 2 Minutes | 416 pages / second |
| Search (queries with most results) | seconds | fast |
| Total data (crawl + search) | 8.5 GB | 170 KB / page |
| Total time | 1 Hour 16 Minutes | 10 pages / second |
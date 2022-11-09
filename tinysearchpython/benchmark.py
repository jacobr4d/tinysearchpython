import sys
import argparse
import numpy as np
import matplotlib.pyplot as plt

# params are args
parser = argparse.ArgumentParser(prog="benchmark crawler", description="analyses crawl log")
parser.add_argument("--log", dest="log_path", default="log", help="where to get log")
args = parser.parse_args(sys.argv[1:])

# get log data in numpy array
data = np.genfromtxt(args.log_path, delimiter=" ")
data[:,0] = data[:,0] - data[0, 0]

# plot frontier, seen_urls vs. time
plt.plot(data[:,0],data[:,1], label="frontier_size")
plt.plot(data[:,0],data[:,2], label="seen_urls_size")
plt.xlabel("seconds")
plt.title("frontier_size and seen_urls_size vs. time")
plt.legend()
plt.show()

# plot robots, count vs. time
plt.plot(data[:,0],data[:,3], label="robots_size")
plt.plot(data[:,0],data[:,4], label="crawled_count")
plt.title("robots_size and crawled_count vs. time")
plt.xlabel("seconds")
plt.legend()
plt.show()

# find rough pages crawled / second
coef = np.polyfit(data[:,0],data[:,4],1)
# poly1d_fn = np.poly1d(coef)
# plt.plot(data[:,0],poly1d_fn(data[:,0]))
print(f"crawled pages / second = ~{coef[0]}")


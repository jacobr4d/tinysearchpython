
import copy
import requests
import time
import logging
import json

from url import *

class Robot:
    """ 
    Records are separated by blank lines
    # is comment
    Fields are of form <field>:<optionalspace><value><optionalspace>
    """
    def __init__(self):
        self.delay = 1
        self.disallowed_paths = []
        self.last_accessed = time.time() - self.delay - 1

    def delays(self):
        return time.time() - self.last_accessed < float(self.delay)
    
    def disallows(self, url):
        return any(url.path.startswith(somepath) for somepath in self.disallowed_paths)

    def update_last_accessed(self):
        self.last_accessed = time.time()

    def __str__(self):
        return json.dumps({"delay": self.delay, "last": self.last_accessed, "disallowed": list(self.disallowed_paths)})

def robot_from_string(robot_string):
    d = json.loads(robot_string)
    robot = Robot()
    robot.delay = d["delay"]
    robot.disallowed_paths = d["disallowed"]
    robot.last_accessed = d["last"]
    return robot

def robot_from_robot_url(robots_url):
    robot = Robot()
    try:
        get = requests.get(robots_url, headers={"user-agent": "tinysearchpython", "connection": "close"}, timeout=1, allow_redirects=True)
    except Exception as e:
        logging.info(f"using default robot: robot request failed: {e}")
        return robot
    get.close()
    user_agent = ""
    for line in get.text.splitlines():
        if not line:
            user_agent = ""
            continue
        
        line = line if "#" not in line else line[:line.index("#")]      # remove comment
        if ":" in line:
            if line.split(":")[0].strip().lower() == "user-agent" and line.split(":")[1].strip() == "*":
                user_agent = "*"
            if line.split(":")[0].strip().lower() == "crawl-delay" and user_agent == "*":
                robot.delay = int(line.split(":")[1].strip())
            if line.split(":")[0].strip().lower() == "disallow" and user_agent == "*":
                robot.disallowed_paths.append(line.split(":")[1].strip())

    # dont want to delay on the request that made us download robots.txt
    robot.last_accessed = time.time() - robot.delay - 1
    return robot

                
if __name__ == "__main__":
    robot_string = robot_string_from_robot_url("https://cv.wikipedia.org:443/robots.txt")
    robot = robot_from_string(robot_string)
    print(str(robot))


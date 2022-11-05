
import copy
import requests
import time

from url import *

class Robot:
    """ 
    Records are separated by blank lines
    # is comment
    Fields are of form <field>:<optionalspace><value><optionalspace>
    """
    def __init__(self, url):
        self.delay = 1
        self.disallowed_paths = set()
        self.last_accessed = time.time()

        robot_url = copy.copy(url)
        robot_url.path = "/robots.txt"
        get = requests.get(robot_url, headers={"user-agent": "tinysearchpython"}, timeout=3)

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
                    self.delay = int(line.split(":")[1].strip())
                if line.split(":")[0].strip().lower() == "disallow" and user_agent == "*":
                    self.disallowed_paths.add(line.split(":")[1].strip())

    def delays(self):
        return time.time() - self.last_accessed < float(self.delay)
    
    def disallows(self, url):
        return any(url.path.startswith(somepath) for somepath in self.disallowed_paths)

                
if __name__ == "__main__":
    robot = Robot(Url("https://google.com"))
    print(robot.delay)
    print(robot.disallowed_paths)
    print(robot.last_accessed)


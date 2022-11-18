
import copy
import requests
import time
import logging
import ujson
import asyncio
from aiohttp import ClientSession, ClientTimeout, ClientError

from reppy.robots import Robots
# from url import *

class Rules:
    """
    Parse robots.txt rules for specific user-agent

    Robots.txt rules:
    Records are separated by blank lines
    # is comment
    Fields are of form <field>:<optionalspace><value><optionalspace>
    """
    def __init__(self, user_agent, default_delay=0):
        self.user_agent = user_agent
        self.delay = default_delay
        self.disallowed = []

    async def fetch_from(self, session, ssl_context, url :str):
        robots_url = Robots.robots_url(url)
        try:
            async with session.get(robots_url, ssl=ssl_context, timeout=10) as response:
                if response.status != 200:
                    logging.debug(f"using default robot: non 200 response: {robots_url}")
                    return
                page = await response.text()
        except Exception as e:
            logging.info(f"get robots exception: {e} {robots_url}")
            return
        reading_rules = False
        saw_specific_rules = False
        saw_general_rules = False
        for line in page.splitlines():
            if not line:
                reading_rules = False
                if saw_specific_rules:
                    return
                continue
            line = line if "#" not in line else line[:line.index("#")]
            if ":" in line:
                if line.split(":")[0].strip().lower() == "user-agent" and line.split(":")[1].strip() == self.user_agent:
                    reading_rules = True
                    saw_general_rules = True
                if line.split(":")[0].strip().lower() == "user-agent" and line.split(":")[1].strip() == "*":
                    reading_rules = True
                    saw_specific_rules = True
                elif reading_rules and line.split(":")[0].strip().lower() == "crawl-delay":
                    self.delay = float(line.split(":")[1].strip())
                elif reading_rules and line.split(":")[0].strip().lower() == "disallow":
                    self.disallowed.append(line.split(":")[1].strip())

    def serialize(self):
        return ujson.dumps({"delay": self.delay, "disallowed": list(self.disallowed)})

    def deserialize_from(self, rules :str):
        _dict = ujson.loads(rules)
        self.delay = _dict["delay"]
        self.disallowed = _dict["disallowed"]

    def disallows(self, url):
        return any(url.path.startswith(somepath) for somepath in self.disallowed)

                
if __name__ == "__main__":
    rules = Rules("jakescrawler")
    rules.fetch_from("https://wikipedia.org")
    print(rules.serialize())
    rules = Rules("jakescrawler")
    rules.fetch_from("https://www.apple.com")
    print(rules.serialize())
    rules = Rules("jakescrawler")
    rules.fetch_from("https://dne.com")
    print(rules.serialize())
import asyncio
from datetime import date, datetime

import httpx
import yaml

from config import cfg

with open("new-domain-rating/config.yaml", "r") as f:
    config = yaml.safe_load(f)
    keywords = config["keywords"]

result_file = open(f"{datetime.today().strftime('%Y-%m-%d')}.txt", "w")

dbdoc = cfg.pymongo_client()["domainwatch"]["domain"]
dt = datetime(date.today().year, date.today().month, date.today().day - 1, 0, 0, 0, 0)

query = {"discovered": {"$gte": dt}, "record_type": "a", "subdomain": ""}
results = dbdoc.find({"subdomain": "", "record_type": {"$in": ["a", "ns"]}, "discovered": {"$gte": dt}})


def process_queue(queue: list, file):
    if not queue:
        return

    response = httpx.post("http://10.10.10.130:2300/score_batch", json={
        "urls": queue
    }, timeout=60)
    data = response.json()
    add_to_watchlist = [key for key, value in data.items() if
                        value["score"] > 0.7 or any(x in value["domain"] for x in keywords)]

    for item in add_to_watchlist:
        print(f"{item} => {data[item]['score']}")
        file.write(f"{item}\n")
        pass
    file.flush()


cache = []
cached_fnc = []
loop = asyncio.get_event_loop()


for result in results:
    cache.append(result["full_domain"])

    if len(cache) > 100:
        cached_fnc.append(process_queue(cache, result_file))
        cache = []
    if len(cached_fnc) > 7:
        f = asyncio.gather(*cached_fnc)
        loop.run_until_complete(f)
        cached_fnc = []
process_queue(cache)

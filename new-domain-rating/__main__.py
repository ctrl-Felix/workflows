import asyncio
import os
from datetime import date, datetime
from dotenv import load_dotenv

import httpx
import yaml
import tldextract

from config import cfg

load_dotenv()

with open("new-domain-rating/config.yaml", "r") as f:
    config = yaml.safe_load(f)
    keywords = config["keywords"]

result_file = open(f"{datetime.today().strftime('%Y-%m-%d')}.txt", "w")

authentication_response = httpx.post(os.getenv("AUTH_ENDPOINT") + "/user/login",
                                     data={'username': os.getenv('SCAMSCAN_USERNAME'),
                                           'password': os.getenv('SCAMSCAN_PASSWORD')
                                           },
                                     )
auth_data = authentication_response.json()
auth_token = auth_data["access_token"]

dbdoc = cfg.pymongo_client()["domainwatch"]["domain"]
dt = datetime(date.today().year, date.today().month, date.today().day - 1, 0, 0, 0, 0)

query = {"discovered": {"$gte": dt}, "record_type": "a", "subdomain": ""}
results = dbdoc.find({"subdomain": "", "record_type": {"$in": ["a", "ns"]}, "discovered": {"$gte": dt}})


def add_to_watchlist(domain):
    domain_data = tldextract.extract(domain)
    request_data = {
        "domain": domain_data.domain,
        "tld": domain_data.suffix,
        "tags": ["scamscan:daily-domain-watch"],
    }
    header = {
        "Authorization": f"Bearer {auth_token}"
    }

    r = httpx.post(f"{os.getenv('BLACKLIST_ENDPOINT')}/watchlist", json=request_data, timeout=60, headers=header)
    if r.status_code >= 300:
        print(f"Error for {domain}")


def process_queue(queue: list, file):
    watchlist_fcn = []
    if not queue:
        return

    response = httpx.post(f"{os.getenv('BLACKLIST_ENDPOINT')}/score_batch", json={
        "urls": queue
    }, timeout=60)
    data = response.json()
    to_watchlist = [key for key, value in data.items() if
                        value["score"] > 0.7 or any(x in value["domain"] for x in keywords)]

    for item in to_watchlist:
        print(f"{item} => {data[item]['score']}")
        add_to_watchlist(item)
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

cached_fnc.append(process_queue(cache, result_file))
loop.run_until_complete(f)

with open(f"{datetime.today().strftime('%Y-%m-%d')}.txt", "r") as f:
    domains = f.read().split('\n')

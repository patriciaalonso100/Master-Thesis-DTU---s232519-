#!/usr/bin/env python3

 

import json

import random

import requests

from datetime import datetime, timedelta, timezone

import math

 

ES_URL = "https://dummy.dk"

API_KEY = "dummy"

 

INDEX = "logs-*"

BATCH_SIZE = 50000

MAX_FILEBEAT_LOGS = 4916

RANDOM_SEED = 7

BUCKET_GRANULARITY = "minute"

SAMPLING_METHOD = "uniform"

 

TIME_RANGE = {

    "gte": "2025-10-16T07:25:51.221Z",

    "lte": "2025-10-16T07:53:51.221Z"

}

 

random.seed(RANDOM_SEED)

 

HEADERS = {

    "Authorization": f"ApiKey {API_KEY}",

    "Content-Type": "application/json"

}

 

base_filter = [

    {"term": {"data_stream.dataset": "elastic_agent.filebeat"}},

    {"range": {"@timestamp": TIME_RANGE}}

]

 

count_query = {

    "size": 0,

    "query": {"bool": {"filter": base_filter}},

    "aggs": {

        "by_minute": {

            "date_histogram": {

                "field": "@timestamp",

                "fixed_interval": "1m",

                "min_doc_count": 0,

                "extended_bounds": {"min": TIME_RANGE["gte"], "max": TIME_RANGE["lte"]}

            }

        }

    }

}

 

resp = requests.post(f"{ES_URL}/{INDEX}/_search", headers=HEADERS, json=count_query)

data = resp.json()

minute_buckets = []

seen_keys = set()

for b in data["aggregations"]["by_minute"]["buckets"]:

    if b["doc_count"] == 0:        

        continue

    key = b["key_as_string"][:16]

   

    if key in seen_keys:        

        continue    

    seen_keys.add(key)

    ts_str =b["key_as_string"]

    start_dt = datetime.strptime(ts_str[:-1], "%Y-%m-%dT%H:%M:%S.%f")    

    end_dt   = start_dt + timedelta(minutes=1)

   

    minute_buckets.append({

        "key": key,        

        "from": ts_str,          

        "to":   end_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z"),                    

        "count": b["doc_count"]    

    })

 

print(f"Found {len(minute_buckets)} non-empty minute buckets")

 

n_buckets = len(minute_buckets)

 

if SAMPLING_METHOD == "uniform":

    per_bucket = math.ceil(MAX_FILEBEAT_LOGS / n_buckets)

    quotas = {b["key"]: per_bucket for b in minute_buckets}

else:  # proportional

    total = sum(b["count"] for b in minute_buckets)

    quotas = {b["key"]: max(1, round(MAX_FILEBEAT_LOGS * b["count"] / total)) for b in minute_buckets}

 

# adjust last bucket down if we overshoot (uniform case)

if SAMPLING_METHOD == "uniform":

    allocated = sum(quotas.values())

    if allocated > MAX_FILEBEAT_LOGS:

        last_key = minute_buckets[-1]["key"]

        quotas[last_key] -= allocated - MAX_FILEBEAT_LOGS

 

all_hits = []

 

for bucket in minute_buckets:

    key = bucket["key"]

    desired = quotas[key]

    total_in_bucket = bucket["count"]

 

    print(f"{key} → {total_in_bucket:,} docs → target {desired}", end=" ")

 

    if total_in_bucket <= desired:

        # just fetch everything

        q = {

            "size": desired,

            "_source": True,

            "query": {

                "bool": {

                    "filter": [

                        {"term": {"data_stream.dataset": "elastic_agent.filebeat"}},

                        {"range": {"@timestamp": {"gte": bucket["from"], "lt": bucket["to"]}}}

                    ]

                }

            },

            "sort": [{"@timestamp": "asc"}]

        }

    else:

        q = {

            "size": desired,

            "_source": True,

            "query": {

                "function_score": {

                    "query": {

                        "bool": {

                            "filter": [

                                {"term": {"data_stream.dataset": "elastic_agent.filebeat"}},

                                {"range": {"@timestamp": {"gte": bucket["from"], "lt": bucket["to"]}}}

                            ]

                        }

                    },

                    "random_score": {

                        "seed": RANDOM_SEED,

                        "field": "_seq_no"      

                    }

                }

            },

            "sort": [{"@timestamp": "asc"}]

        }

 

    r = requests.post(f"{ES_URL}/{INDEX}/_search", headers=HEADERS, json=q)

    hits = r.json().get("hits", {}).get("hits", [])

    all_hits.extend(hits)

    print(f"→ got {len(hits)}")

 

def ts(doc):

    return doc["_source"]["@timestamp"]

 

all_hits.sort(key=ts)

final_sample = all_hits[:MAX_FILEBEAT_LOGS]

 

print(f"\nFinal sample size: {len(final_sample)} (target {MAX_FILEBEAT_LOGS})")

 

with open("filebeat_golden.json", "w") as f:

    json.dump(final_sample, f, indent=2)

 

print("Saved filebeat_golden.json")
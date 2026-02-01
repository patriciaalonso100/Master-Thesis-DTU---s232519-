#!/usr/bin/env python3

 

import json

import random

import requests

from datetime import datetime

 

ES_URL = "https://dummy.dk"

API_KEY = "dummy"

 

INDEX = "logs-*"

MAX_LOGS_PER_SESSION = 50

TARGET_LOGS = 4000

MAX_SESSIONS = 40

BATCH_SIZE = 5000

RANDOM_SEED = 42

random.seed(RANDOM_SEED)

 

HEADERS = {

    "Authorization": f"ApiKey {API_KEY}",

    "Content-Type": "application/json"

}

 

def to_iso8601(date_str):

    dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S.%f")

    return dt.isoformat() + "Z"

 

query = {

    "size": BATCH_SIZE,

    "_source": True,

    "query": {

    "bool": {

      "must": [],

      "filter": [

        {

          "bool": {

            "should": [

              {

                "term": {

                  "data_stream.dataset": {

                    "value": "cisco_ise.log"

                  }

                }

              }

            ],

            "minimum_should_match": 1

          }

        },

        {

          "range": {

            "@timestamp": {

              "format": "strict_date_optional_time",

              "gte": "2025-10-09T08:55:40.705Z",

              "lte": "2025-10-09T09:23:40.705Z"

            }

          }

        }

      ],

      "should": [],

      "must_not": []

    }

  },

    "sort": [{"@timestamp": "asc"}]

}

 

url = f"{ES_URL}/{INDEX}/_search?scroll=2m"

response = requests.post(url, headers=HEADERS, data=json.dumps(query))

data = response.json()

 

print("Query sent:", json.dumps(query, indent=2))

print("Response status:", response.status_code)

print("Response keys:", data.keys())

print("Initial hits:", len(data.get("hits", {}).get("hits", [])))

 

scroll_id = data.get("_scroll_id")

if not scroll_id:

    print("No scroll_id in response:", data)

    exit()

 

all_logs = data.get("hits", {}).get("hits", [])

print(f"Initial batch: {len(all_logs)} logs")

 

while True:

    scroll_url = f"{ES_URL}/_search/scroll"

    scroll_body = {"scroll": "2m", "scroll_id": scroll_id}

    scroll_response = requests.post(scroll_url, headers=HEADERS, data=json.dumps(scroll_body))

    scroll_data = scroll_response.json()

 

    if "hits" not in scroll_data:

        print("Error in scroll response:", scroll_data)

        break

 

    hits = scroll_data["hits"]["hits"]

    if not hits:

        print("No more hits. Scroll finished.")

        break

 

    all_logs.extend(hits)

    scroll_id = scroll_data.get("_scroll_id")

 

    print(f"Fetched {len(hits)} more logs. Total so far: {len(all_logs)}")

 

sessions = {}

for log in all_logs:

    source = log["_source"]

    session = (source.get("cisco_ise", {})

               .get("log", {})

               .get("acs", {})

               .get("session", {})

               .get("id", {}))

    if session:

        session_id = session

    else:

        continue

    sessions.setdefault(session_id, []).append(source)

 

print(f"Created {len(sessions)} sessions.")

for sid, logs in list(sessions.items())[:10]:

    print(f"Session {sid}: {len(logs)} logs")

 

selected_sessions = list(sessions.keys())

random.shuffle(selected_sessions)

 

final_logs = []

remaining_capacity = TARGET_LOGS

for session_id in selected_sessions:    

  if remaining_capacity <= 0:        

    break    

  logs = sessions[session_id]    

  random.shuffle(logs)    

  take = min(len(logs), MAX_LOGS_PER_SESSION, remaining_capacity)    

  final_logs.extend(logs[:take])    

  remaining_capacity -= take

 

# If we still need more, fall back to random global sample

if remaining_capacity > 0 and final_logs:    

  extra = random.sample([log for log in all_logs if log["_source"] not in final_logs],                        

                        min(remaining_capacity, len(all_logs)))    

  final_logs.extend(hit["_source"] for hit in extra)

 

print(f"Sampled {len(final_logs)} logs.")

 

with open("ise_rest_alert8.json", "w") as f:

    json.dump(final_logs[:TARGET_LOGS], f, indent=2)

 

print("Saved ise_alert8.json")
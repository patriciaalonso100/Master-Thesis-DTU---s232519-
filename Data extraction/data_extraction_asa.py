#!/usr/bin/env python3

 

import json

import requests

from datetime import datetime

 

ES_URL = "https://dummy.dk"

API_KEY = "dummy"

 

INDEX = "logs-*"

BATCH_SIZE = 5000

 

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

            "filter": [

              {

                "bool": {

                  "should": [

                    {

                      "term": {

                        "data_stream.dataset": {

                          "value": "cisco_asa.log"

                        }

                      }

                    }

                  ],

                  "minimum_should_match": 1

                }

              },

              {

                "bool": {

                  "should": [

                    {

                      "bool": {

                        "should": [

                          {

                            "match_phrase": {

                              "destination.port": "22"

                            }

                          }

                        ],

                        "minimum_should_match": 1

                      }

                    },

                    {

                      "bool": {

                        "should": [

                          {

                            "match_phrase": {

                              "destination.port": "2222"

                            }

                          }

                        ],

                        "minimum_should_match": 1

                      }

                    }

                  ],

                  "minimum_should_match": 1

                }

              }

            ]

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

        },

        {

          "exists": {

            "field": "cisco.asa.connection_id"

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




scroll_id = data.get("_scroll_id")

if not scroll_id:

    print("No scroll_id in response:", data)

    exit()

 

all_logs = data.get("hits", {}).get("hits", [])

print(f"Initial batch: {len(all_logs)} logs")

 

while True:

    scroll_url = f"{ES_URL}/_search/scroll"

    scroll_body = {"scroll": "10m", "scroll_id": scroll_id}

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

 

with open("asa_alert8.json", "w") as f:

    json.dump(all_logs, f, indent=2)

 

print("Saved asa_alert8.json")
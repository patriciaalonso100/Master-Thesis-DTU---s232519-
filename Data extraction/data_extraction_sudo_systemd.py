#!/usr/bin/env python3

 

import json

import random

import requests

from datetime import datetime

 

ES_URL = "https://dummy.dk"

API_KEY = "dummy"

 

INDEX = "logs-*"

MAX_SUDO_LOGS = 500

MAX_SYSTEMD_LOGS = 500

BATCH_SIZE = 5000

 

HEADERS = {

    "Authorization": f"ApiKey {API_KEY}",

    "Content-Type": "application/json"

}

 

def fetch_system_auth_logs():

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

                          "value": "system.auth"

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

                            "term": {

                              "process.name": {

                                "value": "sudo"

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

                            "term": {

                              "process.name": {

                                "value": "systemd"

                              }

                            }

                          }

                        ],

                        "minimum_should_match": 1

                      }

                    },

                    {

                      "bool": {

                        "filter": [

                          {

                            "bool": {

                              "should": [

                                {

                                  "term": {

                                    "process.name": {

                                      "value": "sshd"

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

                                  "term": {

                                    "event.category": {

                                      "value": "authentication"

                                    }

                                  }

                                }

                              ],

                              "minimum_should_match": 1

                            }

                          }

                        ]

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

 

    scroll_id = data.get("_scroll_id")

    all_logs = data.get("hits", {}).get("hits", [])

 

    while True:

        scroll_url = f"{ES_URL}/_search/scroll"

        scroll_body = {"scroll": "2m", "scroll_id": scroll_id}

        scroll_response = requests.post(scroll_url, headers=HEADERS, data=json.dumps(scroll_body))

        scroll_data = scroll_response.json()

 

        hits = scroll_data.get("hits", {}).get("hits", [])

        if not hits:

            break

 

        all_logs.extend(hits)

        scroll_id = scroll_data.get("_scroll_id")

 

    return [log["_source"] for log in all_logs]

 

def extract_ssh_times(logs):

    ssh_times = []

    for log in logs:

        if log["process"]["name"] == "sshd":

            if "authentication" in log["event"]["category"]:

              ts = log["@timestamp"]

              ssh_times.append(datetime.fromisoformat(ts))

    return ssh_times

 

def sample_sudo_logs(logs, ssh_times):

    sudo_logs = [log for log in logs if log.get("process", {}).get("name") == "sudo"]

 

    def closest_delta(log_time):

        closest_ssh = min(ssh_times, key=lambda ssh: abs((log_time-ssh).total_seconds()))

        delta = abs((log_time - closest_ssh).total_seconds())

        return delta, closest_ssh if ssh_times else float("inf")

 

    for log in sudo_logs:

        log_time = datetime.fromisoformat(log["@timestamp"])

        delta, closest_ssh = closest_delta(log_time)

        log["delta"] = delta

 

    sudo_sorted = sorted(sudo_logs, key=lambda x: x["delta"])

    return sudo_sorted[:MAX_SUDO_LOGS]

 

def sample_systemd_logs(logs):

    systemd_logs = [log for log in logs if log.get("process", {}).get("name") == "systemd"]

    return random.sample(systemd_logs, min(len(systemd_logs), MAX_SYSTEMD_LOGS))

 

def build_final_sample():

    logs = fetch_system_auth_logs()

    ssh_times = extract_ssh_times(logs)

    print(f"HOLAAAA {len(ssh_times)}")

 

    sudo_sampled = sample_sudo_logs(logs, ssh_times)

    #print(sudo_sampled[:20])

    systemd_sampled = sample_systemd_logs(logs)

 

    final_logs = sudo_sampled + systemd_sampled

    print(f"Final sampled logs: {len(final_logs)} (SUDO: {len(sudo_sampled)}, SYSTEMD: {len(systemd_sampled)})")

 

    with open("system_auth_sudo_systemd_alert8.json", "w") as f:

        json.dump(final_logs, f, indent=2)

 

    return final_logs

 

build_final_sample()
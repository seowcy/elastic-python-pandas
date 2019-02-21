import pandas as pd
from elasticsearch import Elasticsearch
import os
import json

# Set ELASTIC_HOST to be the IP address of the elasticsearch coordinator node
ELASTIC_HOST = "127.0.0.1"
es = Elasticsearch(ELASTIC_HOST)

def elastic_simple_query(index,column,query,size=None):
    """
Returns a pandas DataFrame of specified size from the specified index with a search query
on the specified column name.

Args:
    index: name of index in elasticsearch cluster
    column: column name to be searched against
    query: query string to be searched for in the specified column
    size: size of response, returns all responses if None (default: None)
    
    """
    body = {
        "query": {
            "match": {
                column: query
            }
        }
    }
    response = es.search(index=index,body=body,request_timeout=120)
    total_hits = response["hits"]["total"]
    if size == None:
        size = total_hits
    if size <= 10000:
        body = {
            "size": size,
                "query": {
                    "match": {
                        column: query
                    }
                }
        }
        response = es.search(index=index,body=body,request_timeout=120)
        return pd.DataFrame([response["hits"]["hits"][i]["_source"] for i in range(size)])
    if total_hits <= 10000:
        body = {
            "size": total_hits,
                "query": {
                    "match": {
                        column: query
                    }
                }
        }
        response = es.search(index=index,body=body,request_timeout=120)
        return pd.DataFrame([response["hits"]["hits"][i]["_source"] for i in range(total_hits)])
    result = []
    body = {
        "size": 10000,
            "query": {
                "match": {
                    column: query
                }
            }
   }
    response = es.search(index=index,body=body,scroll="10s",request_timeout=120)
    response_hits = len(response["hits"]["hits"])
    result.append(pd.DataFrame([response["hits"]["hits"][i]["_source"] for i in range(response_hits)]))
    scroll_id = response["_scroll_id"]
    for i in range(10000,size,10000):
        response = es.scroll(scroll_id=scroll_id,scroll="10s")
        response_hits = len(response["hits"]["hits"])
        result.append(pd.DataFrame([response["hits"]["hits"][i]["_source"] for i in range(response_hits)]))
    result = pd.concat(result,axis=0,ignore_index=True)
    result["@timestamp"] = pd.to_datetime(result["@timestamp"])
    return result.sort_values("@timestamp", ascending=False).reset_index(drop=True)

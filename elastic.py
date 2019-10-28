import pandas as pd
from elasticsearch import Elasticsearch
import os
import json

# Set ELASTIC_HOST to be the IP address of the elasticsearch coordinator node
ELASTIC_HOST = "127.0.0.1"
es = Elasticsearch(ELASTIC_HOST)
TIMEOUT = 600


def elastic_get_indices(all=False):
    """
Returns a list of all indices (hidden or not hidden) in the elasticsearch cluster.

Args:
    all: if True, returns hidden indices as well (i.e. those prefixed with a period) (default: False)

    """
    if all:
        return sorted(es.cat.indices(h=["index"]).split())
    else:
        return sorted([i for i in es.cat.indices(h=["index"]).split() if i[0] != '.'])

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
        "track_total_hits": True,
        "query": {
            "match": {
                column: query
            }
        }
    }
    response = es.search(index=index,body=body,request_timeout=TIMEOUT)
    total_hits = response["hits"]["total"]["value"]
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
        response = es.search(index=index,body=body,request_timeout=TIMEOUT)
        df = pd.DataFrame([response["hits"]["hits"][i]["_source"] for i in range(size)])
        df["_id"] = [response["hits"]["hits"][i]["_id"] for i in range(size)]
        return df
    if total_hits <= 10000:
        body = {
            "size": total_hits,
                "query": {
                    "match": {
                        column: query
                    }
                }
        }
        response = es.search(index=index,body=body,request_timeout=TIMEOUT)
        df = pd.DataFrame([response["hits"]["hits"][i]["_source"] for i in range(size)])
        df["_id"] = [response["hits"]["hits"][i]["_id"] for i in range(size)]
        return df
    result = []
    body = {
        "size": 10000,
            "query": {
                "match": {
                    column: query
                }
            }
   }
    response = es.search(index=index,body=body,scroll="10s",request_timeout=TIMEOUT)
    response_hits = len(response["hits"]["hits"])
    df = pd.DataFrame([response["hits"]["hits"][i]["_source"] for i in range(response_hits)])
    df["_id"] = [response["hits"]["hits"][i]["_id"] for i in range(response_hits)]
    result.append(df)
    scroll_id = response["_scroll_id"]
    for i in range(10000,size,10000):
        response = es.scroll(scroll_id=scroll_id,scroll="10s")
        response_hits = len(response["hits"]["hits"])
        df = pd.DataFrame([response["hits"]["hits"][i]["_source"] for i in range(response_hits)])
        df["_id"] = [response["hits"]["hits"][i]["_id"] for i in range(response_hits)]
        result.append(df)
    result = pd.concat(result,axis=0,ignore_index=True)
    result["timestamp"] = pd.to_datetime(result["timestamp"])
    return result.sort_values("timestamp", ascending=False).reset_index(drop=True)

def elastic_multi_query(index,query,bool_type="OR",size=None):
    """
Returns a pandas DataFrame of specified size from the specified index with a search query.
Query should be a dictionary of conditions to match.

Args:
    index: name of index in elasticsearch cluster
    query: list of dict of query strings to be searched for in the specified columns (e.g. [{"_id": "abc"}, {"name": "John"}])
    bool_type: specifies whether the query conditions use AND or OR (default: "OR")
    size: size of response, returns all responses if None (default: None)
    
    """
    bool_mapping = {"OR": "should", "AND": "must"}
    body = {
        "track_total_hits": True,
        "query": {
            "bool": {
                bool_mapping[bool_type]: [
                    {"match": i} for i in query 
                ]
            }
        }
    }
    response = es.search(index=index,body=body,request_timeout=TIMEOUT)
    total_hits = response["hits"]["total"]["value"]
    if size == None:
        size = total_hits
    if size <= 10000:
        body = {
            "track_total_hits": True,
            "size": size,
            "query": {
                "bool": {
                    bool_mapping[bool_type]: [
                        {"match": i} for i in query 
                    ]
                }
            }
        }
        response = es.search(index=index,body=body,request_timeout=TIMEOUT)
        df = pd.DataFrame([response["hits"]["hits"][i]["_source"] for i in range(size)])
        df["_id"] = [response["hits"]["hits"][i]["_id"] for i in range(size)]
        return df
    if total_hits <= 10000:
        body = {
            "track_total_hits": True,
            "size": total_hits,
            "query": {
                "bool": {
                    bool_mapping[bool_type]: [
                        {"match": i} for i in query 
                    ]
                }
            }
        }
        response = es.search(index=index,body=body,request_timeout=TIMEOUT)
        df = pd.DataFrame([response["hits"]["hits"][i]["_source"] for i in range(size)])
        df["_id"] = [response["hits"]["hits"][i]["_id"] for i in range(size)]
        return df
    result = []
    body = {
        "track_total_hits": True,
        "size": 10000,
        "query": {
            "bool": {
                bool_mapping[bool_type]: [
                    {"match": i} for i in query 
                ]
            }
        }
    }
    response = es.search(index=index,body=body,scroll="10s",request_timeout=TIMEOUT)
    response_hits = len(response["hits"]["hits"])
    df = pd.DataFrame([response["hits"]["hits"][i]["_source"] for i in range(response_hits)])
    df["_id"] = [response["hits"]["hits"][i]["_id"] for i in range(response_hits)]
    result.append(df)
    scroll_id = response["_scroll_id"]
    for i in range(10000,size,10000):
        response = es.scroll(scroll_id=scroll_id,scroll="10s")
        response_hits = len(response["hits"]["hits"])
        df = pd.DataFrame([response["hits"]["hits"][i]["_source"] for i in range(response_hits)])
        df["_id"] = [response["hits"]["hits"][i]["_id"] for i in range(response_hits)]
        result.append(df)
    result = pd.concat(result,axis=0,ignore_index=True)
    result["timestamp"] = pd.to_datetime(result["timestamp"])
    return result.sort_values("timestamp", ascending=False).reset_index(drop=True)

def elastic_query_range(index,column,start_range,end_range=None,size=None,fields=None):
    """
Returns a pandas DataFrame of specified size and range of a specified column from the specified index.

Args:
    index: name of index in elasticsearch cluster
    column: column name to be searched against
    start_range: the start of the query range
    end_range: the end of the query range, end_range = start_range if end_range == None (default: None)
    size: size of response, returns all responses if None (default: None)
    fields: list of fields to return, returns all fields if None (default: None)
    
    Examples:
    df_date_range = elastic_query_range("paces3","@timestamp","2019-02-01","2019-02-03",None)
    
    """
    
    if end_range is None:
        end_range = start_range
    
    if fields != None:
        if "@timestamp" not in fields:
            fields.append("@timestamp")
    
    body = {
        "query": {
            "range": {
              column: {
                "gte": start_range,
                "lte": end_range
              }
            }
        }
    }
    if fields == None:
        response = es.search(index=index,body=body,request_timeout=TIMEOUT)
    else:
        response = es.search(index=index,body=body,request_timeout=TIMEOUT,_source=fields)
    total_hits = response["hits"]["total"]["value"]
    if size == None:
        size = total_hits
    if size <= 10000:
        body = {
            "size": size,
            "query": {
                "range": {
                  column: {
                    "gte": start_range,
                    "lte": end_range
                  }
                }
            }
        }
        if fields == None:
            response = es.search(index=index,body=body,request_timeout=TIMEOUT)
        else:
            response = es.search(index=index,body=body,request_timeout=TIMEOUT,_source=fields)
        return pd.DataFrame([response["hits"]["hits"][i]["_source"] for i in range(size)])
    if total_hits <= 10000:
        body = {
            "size": total_hits,
            "query": {
                "range": {
                  column: {
                    "gte": start_range,
                    "lte": end_range
                  }
                }
            }
        }
        if fields == None:
            response = es.search(index=index,body=body,request_timeout=TIMEOUT)
        else:
            response = es.search(index=index,body=body,request_timeout=TIMEOUT,_source=fields)
        return pd.DataFrame([response["hits"]["hits"][i]["_source"] for i in range(total_hits)])
    result = []
    body = {
        "size": 10000,
        "query": {
            "range": {
              column: {
                "gte": start_range,
                "lte": end_range
              }
            }
        }
    }
    if fields == None:
        response = es.search(index=index,body=body,scroll="10s",request_timeout=TIMEOUT)
    else:
        response = es.search(index=index,body=body,scroll="10s",request_timeout=TIMEOUT,_source=fields)
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

def elastic_simple_aggs(index,query_type,query_params,aggs_type,aggs_params):
    """
Returns a json (python dictionary) of the specified aggregation result.

Args:
    index: name of index in elasticsearch cluster
    query_type: "match" or "range", no query if None
    query_params:
        "match": {
            "{{name_of_field}}": "{{query}}"
        },
        "range": {
            "{{name_of_field}}": {
                "gte": "{{start_range}}",
                "lte": "{{end_range}}"
            }
        }
    aggs_type: "cardinality", "terms", "date_histogram", "histogram", "min", "max", or "avg"
    aggs_params:
        "cardinality": {
            "field": "{{name_of_field}}"
        },
        "terms": {
            "field": "{{name_of_field}}",
            "size": {{top_n_terms}}
        },
        "date_histogram" {
            "field": "{{name_of_field}}",
            "interval": "{{datetime_interval}}"
        },
        "histogram": {
            "field": "{{name_of_field}}",
            "interval": {{numeric_interval}}
        },
        "min": {
            "field": "{{name_of_field}}"
        },
        "max": {
            "field": "{{name_of_field}}"
        },
        "avg": {
            "field": "{{name_of_field}}"
        }
    
    """
    if query_type != None:
        body = {
            "size": 0,
            "query": {
                query_type: query_params
            },
            "aggs": {}
        }
    else:
        body = {
            "size": 0,
            "aggs": {}
        }
        
    body["aggs"]["my_aggs"] = {
        aggs_type: aggs_params
    }
    
    response = es.search(index=index,body=body,request_timeout=TIMEOUT)
    return response["aggregations"]["my_aggs"]

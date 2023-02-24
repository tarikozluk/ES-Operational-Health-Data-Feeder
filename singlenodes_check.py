import requests
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta, timezone
from re import search

load_dotenv("singlenode_elastics.env")

for i in range(1,8):
    url = os.getenv("ELASTIC_URL_OLD_{}".format(i))+"/_cluster/health/"
    print(url)
    url_main_info = os.getenv("ELASTIC_URL_test_{}".format(i))+"/"
    url_cpu_usage = os.getenv("ELASTIC_URL_test_{}".format(i))+ "/_cat/nodes?v=true&s=cpu:desc&pretty=true"
    url_memory_usage = os.getenv("ELASTIC_URL_test_{}".format(i))+"/_cat/nodes?v=true&s=ram:desc&pretty=true"
    indices_url = os.getenv("ELASTIC_URL_test_{}".format(i))+"/_cat/indices"
    cluster_stats_url = os.getenv("ELASTIC_URL_test_{}".format(i))+"/_cluster/stats"

    try:
        indexer_es = Elasticsearch([os.getenv("INDEXER_ELASTIC_URL")],
                                   basic_auth=(os.getenv("INDEXER_ELASTIC_USER"), os.getenv("INDEXER_ELASTIC_PASSWORD")),
                                   verify_certs=False)
        response = requests.get(url)
        response_cluster_stats = requests.get(cluster_stats_url)
        response_main_info = requests.get(url_main_info)
        response_cpu_usage = requests.get(url_cpu_usage)
        response_memory_usage = requests.get(url_memory_usage)
        response_dict = response.json()

        # todo: variables
        company_tag = url = os.getenv("ELASTIC_URL_test_{}_TAG".format(i))
        timestamp = datetime.now(timezone.utc).isoformat()
        cluster_name = response_dict['cluster_name']
        server_name = response_main_info.json()['name']
        elastic_version = response_main_info.json()['version']['number']
        elastic_health = response_dict['status']
        number_of_nodes = response_dict['number_of_nodes']
        active_primary_shards = response_dict['active_primary_shards']
        size_in_gb = response_cluster_stats.json()['nodes']['fs']['total_in_bytes'] / 1024 / 1024 / 1024
        free_in_gb = response_cluster_stats.json()['nodes']['fs']['free_in_bytes'] / 1024 / 1024 / 1024
        active_shards = response_dict['active_shards']
        relocating_shards = response_dict['relocating_shards']
        initializing_shards = response_dict['initializing_shards']
        unassigned_shards = response_dict['unassigned_shards']
        delayed_unassigned_shards = response_dict['delayed_unassigned_shards']
        number_of_pending_tasks = response_dict['number_of_pending_tasks']
        number_of_in_flight_fetch = response_dict['number_of_in_flight_fetch']
        task_max_waiting_in_queue_millis = response_dict['task_max_waiting_in_queue_millis']
        active_shards_percent_as_number = response_dict['active_shards_percent_as_number']
        indices_count = response_cluster_stats.json()['indices']['count']
        cpu_utilization = response_cluster_stats.json()['nodes']['process']['cpu']['percent']
        heap_used_in_gb = response_cluster_stats.json()['nodes']['jvm']['mem'][
                              'heap_used_in_bytes'] / 1024 / 1024 / 1024
        docs_count = response_cluster_stats.json()['indices']['docs']['count']
        lucene_version = response_main_info.json()['version']['lucene_version']
        segments_memory_in_gb = response_cluster_stats.json()['indices']['segments'][
                                    'memory_in_bytes'] / 1024 / 1024 / 1024
        segments_terms_memory_in_gb = response_cluster_stats.json()['indices']['segments'][
                                          'terms_memory_in_bytes'] / 1024 / 1024 / 1024
        indexer_es.index(
            index=("eol-metricshealth-{}-monitoring_{}".format(company_tag, datetime.now().strftime("%Y.%m.%d"))).lower(),

            document={
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "company_tag": company_tag,
                "cluster_name": cluster_name,
                "server_name": server_name,
                "elastic_version": elastic_version,
                "elastic_health": elastic_health,
                "number_of_nodes": number_of_nodes,
                "active_primary_shards": active_primary_shards,
                "size_in_gb": size_in_gb,
                "free_in_gb": free_in_gb,
                "active_shards": active_shards,
                "relocating_shards": relocating_shards,
                "initializing_shards": initializing_shards,
                "unassigned_shards": unassigned_shards,
                "delayed_unassigned_shards": delayed_unassigned_shards,
                "number_of_pending_tasks": number_of_pending_tasks,
                "number_of_in_flight_fetch": number_of_in_flight_fetch,
                "task_max_waiting_in_queue_millis": task_max_waiting_in_queue_millis,
                "active_shards_percent_as_number": active_shards_percent_as_number,
                "indices_count": indices_count,
                "cpu_utilization": cpu_utilization,
                "heap_used_in_gb": heap_used_in_gb,
                "docs_count": docs_count,
                "lucene_version": lucene_version,
                "segments_memory_in_gb": segments_memory_in_gb,
                "usegments_term_memory_in_gb": segments_terms_memory_in_gb,
                # "Nodes": es.cat.nodes(),
            })
    except Exception as e:
        print("Hatalı")

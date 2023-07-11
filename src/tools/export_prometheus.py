import json
from jsonpointer import resolve_pointer
import time
import random
from os import path
import yaml
from prometheus_client import start_http_server
from prometheus_client.core import GaugeMetricFamily, REGISTRY, InfoMetricFamily


def get_metrics(data, concat, prefix=""):
    keys = []

    if isinstance(data, dict):
        for key, value in data.items():
            new_prefix = f"{prefix}{concat}{key}" if prefix else f"{concat}{key}" if concat == '/' else f"{key}"
            keys.extend(get_metrics(value, concat, new_prefix))

    elif isinstance(data, list):
        for index, value in enumerate(data):
            new_prefix = f"{prefix}{concat}{index}" if prefix else f"{concat}{index}" if concat == '/' else f"{index}"
            keys.extend(get_metrics(value, concat, new_prefix))

    else:
        keys.append(prefix)

    return keys


class RandomNumberCollector(object):
    def __init__(self):
        pass

    def collect(self):
        file = open('~/statistics.json')
        data = json.load(file)
        metrics_names = get_metrics(data, '_')
        metrics_keys = get_metrics(data, '/')
        metrics_docs = get_metrics(data, ' ')

        for i in range(len(metrics_names)):
            result = resolve_pointer(data, metrics_keys[i])
            metrics_names[i] = metrics_names[i].replace('.', '_')
            if isinstance(result, str):
                info = InfoMetricFamily(metrics_names[i], metrics_docs[i])
                info.add_metric(metrics_names[i], {metrics_names[i]: result})
                yield info
            else:
                gauge = GaugeMetricFamily(metrics_names[i], metrics_docs[i])
                gauge.add_metric(metrics_names[i], result)
                yield gauge


if __name__ == "__main__":
    start_http_server(9777)
    REGISTRY.register(RandomNumberCollector())
    while True:
        # period between collection
        time.sleep(1)

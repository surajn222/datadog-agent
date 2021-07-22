from datadog import initialize, statsd
import time
from master_hbasemetrics import *

class hbase_metrics:
    def __init__(self, list_metrics):
        self.list_metrics = list_metrics

        self.options = {
            'statsd_host':'127.0.0.1',
            'statsd_port':8125
        }

    def initialize(self):
        print("Initializing")
        initialize(**self.options)

    def service_check(self):
        print("Service Checks")
        statsd.service_check(
            check_name="HBaseMaster.service_check",
            status="0",
            message="Application is OK",
        )

    def fetch_and_push_metrics(self, port):
        print("Pushing Metrics")
        for metric in fetch_metrics("http://localhost:" + str(port)):
            print(metric['metric'])
            if str(metric['metric']) in self.list_metrics:
                print("Pushing METRIC:" + str(metric['metric']) + ": " + str(metric['value']))
                statsd.gauge(metric['metric'], metric['value'],["{}:{}".format(k, v) for k, v in metric.get('tags', {}).items()])

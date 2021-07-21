from datadog import initialize, statsd
import time
from master_hbasemetrics import *

options = {
    'statsd_host':'127.0.0.1',
    'statsd_port':8125
}

initialize(**options)

statsd.service_check(
    check_name="HBaseMaster.service_check",
    status="0",
    message="Application is OK",
)

list_metrics = ["metric1", "metric2"]

for metric in fetch_metrics("localhost:16030"):
    print("METRIC:" + str(metric['metric']) + ": " + str(metric['value']))
   #statsd.gauge(metric['metric'], metric['value'],["{}:{}".format(k, v) for k, v in metric.get('tags', {}).items()])

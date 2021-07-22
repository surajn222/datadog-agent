#!/usr/bin/env python3
from datadog import initialize, statsd
import time
from master_hbasemetrics import *
from hbase_metrics import *
from s3_metrics import *
import sys
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

obj_hbase_metrics = hbase_metrics()
obj_hbase_metrics.initialize()
obj_hbase_metrics.service_check()
obj_hbase_metrics.fetch_and_push_metrics()

sys.exit()

bucket_name = config['s3_metrics']['bucket']
prefix = config['s3_metrics']['prefix']
tag = config['s3_metrics']['tag']

obj_s3_metrics = s3_metrics
obj_s3_metrics.initialize()
obj_s3_metrics.service_check()
obj_s3_metrics.connect()
obj_s3_metrics.fetch_and_push_metrics(bucket_name, prefix, tag)


#
# options = {
#     'statsd_host':'127.0.0.1',
#     'statsd_port':8125
# }
#
# initialize(**options)
#
# statsd.service_check(
#     check_name="HBaseMaster.service_check",
#     status="0",
#     message="Application is OK",
# )
#
# list_metrics = ["hbase.master.server.numRegionServers",
# "hbase.master.server.numDeadRegionServers",
# "hbase.regionserver.server.readRequestCount",
# "hbase.regionserver.server.writeRequestCount",
# "hbase.jvmmetrics.GcTimeMillis",
# "hbase.jvmmetrics.MemHeapUsedM",
# "hbase.jvmmetrics.MemHeapMaxM",
# "hbase.regionserver.server.compactionQueueLength",
# "hbase.regionserver.ipc.numCallsInGeneralQueue",
# "hbase.regionserver.server.Get_95th_percentile",
# "hbase.regionserver.io.FsPReadTime_95th_percentile",
# "hbase.regionserver.io.FsWriteTime_95th_percentile"]
#
# for metric in fetch_metrics("localhost:16030"):
#
#     if str(metric['metric']) in list_metrics:
#         print("METRIC:" + str(metric['metric']) + ": " + str(metric['value']))
#         statsd.gauge(metric['metric'], metric['value'],["{}:{}".format(k, v) for k, v in metric.get('tags', {}).items()])

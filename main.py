#!/usr/bin/env python3
from datadog import initialize, statsd
import time
from master_hbasemetrics import *
from hbase_metrics import *
from s3_metrics import *
import sys
import configparser
import json

config = configparser.ConfigParser()
config.read('config.ini')

#check if server is master or slave
f = open("/mnt/var/lib/info/instance.json", "r")
#f = open("instance.json", "r")
str_instance_details = f.read()

json_instance_details = json.loads(str_instance_details)
str_is_master = json_instance_details["isMaster"]

print(str_is_master)

if str_is_master==True:
    master = True
else:
    master = False

if master:
    port = 16010
else:
    port = 16030

print("Port: " + str(port))
obj_hbase_metrics = hbase_metrics()
obj_hbase_metrics.initialize()
obj_hbase_metrics.service_check()
obj_hbase_metrics.fetch_and_push_metrics(port)

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

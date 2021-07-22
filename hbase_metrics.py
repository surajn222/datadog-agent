from datadog import initialize, statsd
import time
from master_hbasemetrics import *

class hbase_metrics:
    def __init__(self):
        self.list_metrics = ["hbase.master.server.numRegionServers",
                            "hbase.master.server.numDeadRegionServers",
                            "hbase.master.assignmentmanger.ritCount",
                            "hbase.regionserver.server.readRequestCount",
                            "hbase.regionserver.server.writeRequestCount",
                            "hbase.jvmmetrics.GcTimeMillis",
                            "hbase.jvmmetrics.MemHeapUsedM",
                            "hbase.jvmmetrics.MemHeapMaxM",
                            "hbase.regionserver.server.compactionQueueLength",
                            "hbase.regionserver.ipc.numCallsInGeneralQueue",
                            "hbase.regionserver.server.Get_95th_percentile",
                            "hbase.regionserver.io.FsPReadTime_95th_percentile",
                            "hbase.regionserver.io.FsWriteTime_95th_percentile",
                            "hbase.regionserver.io.FsPReadTime_95th_percentile",
                            "hbase.regionserver.io.FsWriteTime_95th_percentile"
                             ]

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
        for metric in fetch_metrics("localhost:" + str(port)):
            if str(metric['metric']) in self.list_metrics:
                print("Pushing METRIC:" + str(metric['metric']) + ": " + str(metric['value']))
                statsd.gauge(metric['metric'], metric['value'],["{}:{}".format(k, v) for k, v in metric.get('tags', {}).items()])

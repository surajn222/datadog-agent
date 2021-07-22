import boto3
from datadog import initialize, statsd
import time

class s3_metrics:
    def __init__(self):
        self.options = {
            'statsd_host':'127.0.0.1',
            'statsd_port':8125
        }

        print("Initializing")
        initialize(**self.options)

    def service_check(self):
        statsd.service_check(
            check_name="HBaseMaster.service_check",
            status="0",
            message="Application is OK",
        )

    def connect(self):
        self.s3 = boto3.client('s3')

    def fetch_and_push_metrics(self, bucket_name, prefix, tag):
        table=["sampletable1","sampletable2","sampletable3","usertable1","usertable2","usertable3","usertable4","user_preference"]
        size=0
        count=0
        paginator = self.s3.get_paginator('list_objects_v2')
        for i in table:
            size=0
            count=0
            pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix+i+'/')
            for page in pages:
               for obj in page['Contents']:
                 size+=obj['Size']
                 count+=1
            print("Pushing metric: " + i + " Size:" + size + " Count:" + count)
            statsd.gauge('aws_s3.tablesize',size,tags=[tag,"table:"+i])
            statsd.gauge('aws_s3.num_of_objects_in_table',count,tags=[tag,"table:"+i])

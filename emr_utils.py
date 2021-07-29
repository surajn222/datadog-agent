import os
import sys
import json

def identify_master_node():
    #check if server is master or slave
    f = open("/mnt/var/lib/info/instance.json", "r")
    #f = open("instance.json", "r")
    str_instance_details = f.read()
    json_instance_details = json.loads(str_instance_details)
    str_is_master = json_instance_details["isMaster"]
    return str_is_master

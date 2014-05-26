#!/usr/bin/python

import sys
import string
import re
import os

def get_fs():
    points = []
    base=sys.argv[1]
    with open('/proc/mounts', 'r') as f:
        for line in f:
            r     = line.split(' ')
            dev   = r[0]
            point = r[1]
            fs    = r[2]
            opts  = r[3].split(',')

            rw = False
            for o in opts:
                if o == "rw":
                    rw = True
                    break

            if dev.startswith('/dev/') and point.startswith(base) and rw:
                try:
                    f=open(point+"/touch", "w")
                    f.close()
                    points.append(point)
                except IOError:
                    pass

    return points

def get_dclist(mydc):
    res = []
    myzk = ""
    with open('/etc/yandex/statbox-kafka/dc-map', 'r') as f:
        for line in f:
            line = line.strip()
            r = line.split("\t")
            dc    = r[0]
            zk    = r[1]

            res.append(dc + ";" + zk)

            if (dc == mydc):
                myzk = zk

    return (res, myzk)

def generate():
    points  = get_fs()
    log_dir = ",".join(map(lambda x: x + "/kafka", points))
    broker_id = int(sys.argv[3])
    dc        = sys.argv[4]
    yamr      = "redir-log,reqans-log,img-reqans-log,xml-reqans-log,archive2-log,archive2-push-log,mobile-app-reqans-log,tskv-log,unparsed-log"

    dc_vec, myzk = get_dclist(dc)
    dc_list = "|".join(dc_vec)

    if len(points) == 0:
        sys.exit(-1)

    for point in points:
        os.system("chown -R statbox:statbox " + point + "/kafka")


    with open(sys.argv[2], 'r') as f:
        content = f.read()
        tmpl = string.Template(content)
        print tmpl.safe_substitute(log_dir=log_dir, broker_id=broker_id, DC=dc, DCLIST=dc_list, yamr=yamr, myzk=myzk)

generate()


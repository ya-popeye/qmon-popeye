#!/usr/bin/python
import sys
import string
import re
import os
import random

#dry_run = True
dry_run = False

class Log:
    def __init__(self, point, topic):
        self.point           = point
        self.topic           = topic
        r = re.compile(r"([0-9]+)\.log")

        self.last_segment_no = 0

        for filename in os.listdir(point + "/kafka/" + topic):
            m = r.match(filename)
            if m:
                seg_no = int(m.group(1))
                if seg_no > self.last_segment_no:
                    self.last_segment_no = seg_no

        print "loaded %s %s %d" % (point, topic, self.last_segment_no)

    def __cmp__(self, other):
        return other.last_segment_no - self.last_segment_no

    def delete(self):
        cmd = "rm -rf " + self.point + "/kafka/" + self.topic
        print cmd

        if not dry_run:
            r = os.system(cmd)
            if not r == 0:
                raise Exception(cmd)

    def move(self, new_point):
        cmd = "rsync -avv " + self.point + "/kafka/" + self.topic + " " + new_point + "/kafka/"
        print cmd

        if not dry_run:
            r = os.system(cmd)
            if not r == 0:
                raise Exception(cmd)
        self.delete()
        self.point = new_point


def get_fs(base):
    points = []
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

class Point:
    def __init__(self, point):
        self.point = point
        self.logs  = []

    def add(self, log):
        self.logs.append(log)

    def pop(self):
        random.shuffle(self.logs)
        return self.logs.pop()

    def size(self):
        return len(self.logs)
    
    def __cmp__(self, other):
        return len(other.logs) - len(self.logs)
    
def process():
    if len(sys.argv) > 1:
        points = get_fs(sys.argv[1])
    else:
        points = ["points/1", "points/2", "points/3"]

    topic2log = {}

    point2point = {}
    ppoints     = []

    # load logs info
    for point in points:
        p = Point(point)
        ppoints.append(p)
        point2point[point] = p

        for filename in os.listdir(point + "/kafka"):
            if os.path.isdir(point + "/kafka/" + filename):
                log = Log(point, filename)

                if not topic2log.has_key(log.topic):
                    topic2log[log.topic] = []

                topic2log[log.topic].append(log)

    # process doubles
    total_logs  = []

    for topic, logs in topic2log.items():
        logs.sort()
        while len(logs) > 1:
            log = logs.pop()
            log.delete()

        total_logs.append(logs[0])
    
    # process empty partitions
    for log in total_logs:
        point = log.point
        point2point[point].add(log)
    
    ppoints.sort()
    for p in ppoints:
        print p.point, p.size()

    dif = len(total_logs) % len(ppoints)
    if dif > 1:
        dif = 1

    while True:
        mx = ppoints[0].size()

        mod = False
        p = ppoints.pop()
        print mx, ppoints[0].point, p.size(), p.point

        if mx - p.size() > dif:
            last = ppoints[0]
            log = last.pop()

            log.move(p.point)
            p.add(log)

            mod = True

        ppoints.append(p)

        ppoints.sort()

        if mod == False:
            break

process()


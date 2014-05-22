#!/usr/bin/python

import os
import os.path
import glob

droplogs = {
        "/var/log/statbox/logbroker-streamprocessor.log": 48,
        "/var/log/statbox/logbroker-import.log": 16,
        "/var/log/statbox/logbroker-export.log": 16,
        "/var/log/statbox/kafka-log-cleaner.log": 12,
        "/var/log/statbox/kafka-request.log": 12,
        "/var/log/statbox/kafka-server.log": 12,
        "/var/log/statbox/kafka-state-change.log": 5,
        }

def clean_log(base, files, full):
    #d = os.path.dirname(base)
    #print d 

    os.system("chown statbox:statbox %s" % (base))

    ff = glob.glob(base + ".*")
    ff.sort()
    ff.reverse()
    fg = ff[:files]
    ff = ff[files:]

    for f in ff:
        print "drop " + f
        os.unlink(f)

    if not full and fg:
        fg = fg[1:]
        
    for f in fg:        
        if not f.endswith(".gz"):
            print "compress " + f
            os.system("gzip " + f)
            os.system("chown statbox:statbox %s" % (f + ".gz"))


for l, files in droplogs.iteritems():
    clean_log(l, files, False)


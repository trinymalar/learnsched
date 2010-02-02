#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import tempfile
import urllib
import random
import threading
import time

TASK_RUNTIME = 60 # in seconds
CPU_ACT_LIM = 2**8
DISK_ACT_LIM = 4096 * 4
MEM_ACT_LIM = 10000 
REPORT_TIME = 5
MAX_ACTS = 5
CONTINUE_ACTIVITY = True

WORK_TIME_U = 1.0
WORK_TIME_VAR = 0
SLEEP_WORK_RATIO = 3 

urls = []
# Example: "http://10.2.4.181/websim/"]
 

NET_FILE_COUNT = 499


def cpuact():
    modz = 0
    for i in xrange(1, CPU_ACT_LIM):
      for j in xrange(1, i):
        modz = i % j 
    return "C"

def diskact():
    dsz = random.randint(1, DISK_ACT_LIM)
    data = range(dsz)
    try:
        f = tempfile.TemporaryFile()
        f.write(str(data))
        f.close()
    except IOError, e:
        sys.stderr.write(e)
    return "D"
            
def memact():
    dsz = random.randint(1, MEM_ACT_LIM)
    data = range(dsz)
    del data
    return "M"

def sleepact():
    time.sleep(0.5)

def netact():
    urltoget = random.choice(urls) + str(random.randint(0, NET_FILE_COUNT)) + ".txt"
    try:
        f = urllib.urlopen(urltoget, proxies={})
        f.read()
        f.close()
    except:
        sys.stderr.write("Error ocurred in netact\n")


def stopact():
  global CONTINUE_ACTIVITY
  CONTINUE_ACTIVITY = False
  sys.stderr.write("reporter:status:Stopping activity\n")

           
def doactivity(activity, work_time):
    #sleep_time = work_time * SLEEP_WORK_RATIO
    #if sleep_time : time.sleep(sleep_time)
    i = 0
    work_start_time = time.time()
    while (time.time() - work_start_time) <= work_time:
        activity()
        i = i + 1
    return i


low_activity_weights = {
    cpuact : 0.1,
    sleepact: 0.8, 
    diskact : 0.04,
    memact: 0.03,
    netact : 0.03
}

medium_activity_weights = {
  cpuact : 0.3,
  sleepact: 0.6, 
  diskact : 0.04,
  memact: 0.03,
  netact : 0.03
}
  
high_activity_weights = {
    cpuact : 0.7,
    sleepact: 0.2, 
    diskact : 0.04,
    memact: 0.03,
    netact : 0.03
}

class ProgressReporter(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self.setName("Progress Reporter")
        
    def run(self):
        while 1:
            time.sleep(REPORT_TIME)
            sys.stderr.write("reporter:status:IAMALIVE\n")

def main():
    th = ProgressReporter()
    th.start()
    tmr = threading.Timer(TASK_RUNTIME, function=stopact)
    tmr.start()
    activity_weights = low_activity_weights
    for line in sys.stdin:
        if CONTINUE_ACTIVITY:
            counts = []
            for act, wt in activity_weights.iteritems():
                if wt > 0 : counts.append(doactivity(act, REPORT_TIME * wt))
            print time.time(),'\t',counts


if __name__ == "__main__":
    main()
    

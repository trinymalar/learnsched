#!/usr/bin/env python
import sys
import tempfile
import random
import threading
import time

TASK_RUN_TIME = 60 # in seconds
CPU_ACT_LIM = 2**10
DISK_ACT_LIM = 4096 * 4
MEM_ACT_LIM = 10000 
REPORTER_STARTED = False
SLEEP_TIME = 15
MAX_ACTS = 5
CONTINUE_ACTIVITY = True

def cpuact():
    for i in xrange(1, random.randint(1, CPU_ACT_LIM)):
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
    
def stopact():
  global CONTINUE_ACTIVITY
  CONTINUE_ACTIVITY = False
  sys.stderr.write("reporter:status:Stopping activity\n")

class ProgressReporter(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self.setName("Progress Reporter")
        
    def run(self):
        while 1:
            time.sleep(SLEEP_TIME)
            sys.stderr.write("reporter:status:IAMALIVE\n")
                
def main():
    th = ProgressReporter()
    th.start()
    activities = [cpuact]
    tmr = threading.Timer(TASK_RUNTIME, function=stopact)
    tmr.start()
    for line in sys.stdin:
        numacts = random.randint(1, MAX_ACTS)
        actstr = []
        if CONTINUE_ACTIVITY:
          for i in xrange(numacts):
            act = random.choice(activities)
            actstr.append(act())
          print "%s\t%s" % (time.asctime().replace(" ", "_"), "_".join(actstr))
            
if __name__ == "__main__":
    main()
    

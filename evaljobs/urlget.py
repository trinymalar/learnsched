#!/usr/bin/env python
import sys
import urllib
import random
import threading
import time

TASK_RUNTIME = 60
SLEEP_TIME_MAX = 2


urls = [ "http://10.2.4.181/websim/", 
         "http://10.2.4.162/websim/", 
         "http://10.2.4.162/websim/"]
print urls
i = 0
CONTINUE_ACTIVITY = True

def stopact():
  global CONTINUE_ACTIVITY
  CONTINUE_ACTIVITY = False
  sys.stderr.write("reporter:status:Stopping activity\n")

tmr = threading.Timer(TASK_RUNTIME, function=stopact)
tmr.start()

for line in sys.stdin:
  i = i + 1
  if i % 100 == 0 : sys.stderr.write("reporter:progress:100-more-records\n")
  if i % 10 == 0 and CONTINUE_ACTIVITY:
    try:
        sleep_time = random.uniform(1, SLEEP_TIME_MAX)
        print "will sleep for", sleep_time
        time.sleep(sleep_time)
        urltoget = random.choice(urls) + str(random.randint(0, 499)) + ".txt"
        # don't use any proxy
        f = urllib.urlopen(urltoget, proxies={})
        urlcont = f.read().replace('\n','_')
        #print "%s\t%s" % (urltoget, urlcont)
        print "%s\t%d\t%d" % (urltoget, i, len(urlcont))
        f.close()
    except IOError, msg:
        sys.stderr.write("Error while retrieving URL: %s\n" % msg)



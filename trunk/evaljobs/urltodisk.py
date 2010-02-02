#!/usr/bin/env python
import sys
import urllib
import threading
import time
import tempfile
import random
TMP_FNAME = "big_temp_file.dat"
SLEEP_TIME = 15
urls = [] # include urls to be downloaded here.

DOWNLOAD_COUNT = 2

class ProgressReporter(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self) 
        self.setDaemon(True)
        self.setName("Progress Reporter")
        
    def run(self):
        while 1:
            time.sleep(SLEEP_TIME)
            sys.stderr.write("reporter:status:IAMALIVE\n")

class Downloader(threading.Thread):
    def __init__(self):
      threading.Thread.__init__(self)
      self.setName("Downloader")
      self.setDaemon(False)
    
    def run(self):
      for i in range(DOWNLOAD_COUNT):
        self.download()
    
    def download(self):
      try:
            f = urllib.urlopen(random.choice(urls), proxies={}) # don't use any proxy
            fw = tempfile.TemporaryFile()
            fw.write(f.read())
            fw.close()
            f.close()
            print "%s\t%s" %(time.asctime().replace(" ","_"), URL)
      except IOError, e:
            sys.stderr.write("IOERROR%s\n" % e)

            
def main():
    REPORTER_STARTED = 0 
    i = 0
    for line in sys.stdin:
        i = i + 1
        k = 2**16 % i
        if not REPORTER_STARTED:
            th = ProgressReporter()
            th.start()
            thdl = Downloader()
            thdl.start()
            REPORTER_STARTED = 1
                      


if __name__ == "__main__":
    main()

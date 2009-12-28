#!/usr/bin/env python

import statgrab
from statgrab import *
import urllib2, urllib, time, math, sys
from urllib2 import URLError
sg_init()

SLEEPING_INTERVAL = 3 

REPORT_RESOURCES = ["cpu_used",
                    "mem_used",
                    "diskio",
                    "netio"
                    ]

W = 1
alpha = 1 - math.exp(- (SLEEPING_INTERVAL) / (W * 60 ))
#print alpha
stats = {}


def get_sys_stats():
    ret = {}
    # cpu load average
    ret['load'] = sg_get_load_stats()['min1']
    #  CPU statistics
    dcpu = sg_get_cpu_percents()
    ret['cpu_kernel'] = float(dcpu['kernel'])
    ret['cpu_user'] = float(dcpu['user'])
    ret['cpu_iowait'] = float(dcpu['iowait'])
    ret['cpu_used'] = float(ret['cpu_kernel'] + ret['cpu_user'] + ret['cpu_iowait']) / 10
    # disk stats
    total_read_bytes = 0
    total_write_bytes = 0
    # need to loop over all available disks
    for d in sg_get_disk_io_stats_diff():
      total_read_bytes += d['read_bytes'] 
      total_write_bytes += d['write_bytes' ]
    ret['disk_read'] = float(total_read_bytes) / (1024 * 1024 * 1024)
    ret['disk_write'] = float(total_write_bytes) / (1024 * 1024 * 1204) 
    ret['diskio'] = ret['disk_read'] + ret['disk_write']
    # memory stats
    mem_free = sg_get_mem_stats()['free']
    mem_total = sg_get_mem_stats()['total']
    ret['mem_used'] = float(mem_total - mem_free) / (1024 * 1024 * 1024)
    
    # network I/O
    total_tx = 0
    total_rx = 0
    for d in sg_get_network_io_stats_diff():
      total_tx += d['tx']
      total_rx += d['rx']
    ret['network_tx'] = float(total_tx)/(10 * 1024 * 1024)
    ret['network_rx'] = float(total_rx)/(10 * 1024 * 1024)
    ret['netio'] = ret['network_tx'] + ret['network_rx']

    # processes
    ret['proc_total'] = sg_get_process_count()['total']
    ret['proc_running'] = sg_get_process_count()['running']
    ret['proc_sleeping'] = sg_get_process_count()['sleeping']

    return ret

def stats_moving_average(smoothing):
    global stats
    for k,v in get_sys_stats().iteritems():
      alp = 1
      if stats.has_key(k):
        if smoothing: alp = alpha 
        stats[k] = alp * v + (1-alp) * stats[k] 
      else:
        stats[k] = 0
    return stats


def stated_main():
    smoothing = "-s" in sys.argv
    while 1:
      tstats = stats_moving_average(smoothing)
      repline = "\t".join([str(round(tstats[k],2)) for k in sorted(REPORT_RESOURCES)])
      sys.stdout.write(repline+"\n");
      sys.stdin.read(1)
      sys.stdout.flush()
      #time.sleep(SLEEPING_INTERVAL)

if __name__ == "__main__":
  try:
    stated_main()
  except KeyboardInterrupt, intr:
    sys.stderr.write("\nstopping stated ...\n")

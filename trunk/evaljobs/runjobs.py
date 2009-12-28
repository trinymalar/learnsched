#!/usr/bin/env python
from __future__ import with_statement
import random, time, threading, sys, subprocess


# Hadoop paths
HADOOP_HOME = "/home/hdev/hadoop-0.20.0"
HADOOP_STREAMING_JAR = HADOOP_HOME + "/contrib/hadoop-0.20.0-streaming.jar"
# job submission rate
JOB_SUBMIT_RATE = 5 
# number of times the workload has to be run
LOOP_CNT = 5 
# time difference between two workloads
WORKLOAD_GAP = 5

class HadoopJob(threading.Thread):    
    def __init__(self, jobname="", cmdstr=""):
        threading.Thread.__init__(self)
        self.jobname = jobname
        self.cmdstr = cmdstr
      
    def exec_job_proc(self):
        try:
            subprocess.Popen(args=self.cmdstr, shell=True).wait()
            #print self.cmdstr # For testing
        except IOError, e :
            sys.stderr.write("Error: %s\n" % e)
    
    def run(self):
        before = time.time()
        self.exec_job_proc()
        now = time.time()
        self.job_run_time = now - before
        print self.jobname, self.job_run_time

def read_jobs(job_list_file):
    jobs = []
    input_job = HadoopJob()
    with open(job_list_file) as jobfile:
        for line in jobfile:
            jobname, job_cmd_str = line.strip().split("::")
            print "Adding:[%s]" % jobname
            job = HadoopJob(jobname, job_cmd_str)
            if jobname == "inputgen":
              print "Input job:",job.jobname
              input_job = job
            else:
                jobs.append(job)
        return (input_job, jobs)
    return None

def hdfs_cleanup():
    import getpass
    uname = getpass.getuser()
    try:
        hdfs_cleanup_cmd = HADOOP_HOME +\
         """/bin/hadoop dfs -rmr "/user/%s/*" """ % uname
        subprocess.Popen(args=hdfs_cleanup_cmd, shell=True).wait()
        #print hdfs_cleanup_cmd
        print "Cleaned HDFS"
    except IOError, e:
        sys.stderr.write("Error: %s\n" % e)

def main():
    if len(sys.argv) != 2:
        sys.stderr.write("Usage %s <Hadoop job list file>\n" % sys.argv[0])
        sys.exit(1)
    # read the jobs from joblist file
    input_job, jobs = read_jobs(sys.argv[1])    
    # exit if no jobs to run
    if not jobs and not input_job:
        sys.stderr.write("No jobs to execute\n")
        sys.exit(1)        
    # run the workload LOOP_CNT times
    for i in xrange(LOOP_CNT):
        input_job, jobs = read_jobs(sys.argv[1])
        print "*"*20,i,"*"*20
        before = time.time()
        # first create random text input
        input_job.start()
        input_job.join()
        # randomly execute jobs according to exponential distribution
        random.shuffle(jobs)
        for job in jobs:
            sleep_time = random.expovariate(1.0/JOB_SUBMIT_RATE)
    #        time.sleep(sleep_time)
            job.start()
            job.join()
        now = time.time()
        # clear hdfs
        hdfs_cleanup()
        # need to wait some time so that all load values go down
        time.sleep(WORKLOAD_GAP)
    
if __name__ == "__main__":
    main()

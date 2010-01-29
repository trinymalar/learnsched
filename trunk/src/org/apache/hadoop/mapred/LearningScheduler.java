package org.apache.hadoop.mapred;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.ReflectionUtils;

public class LearningScheduler extends TaskScheduler {

  // list of jobs we are scheduling
  private ArrayList<JobInProgress> joblist;
  // utility function that computes utility of job
  private UtilityFunction utilFunc;
  // classifier object
  private Classifier classifier;
  private HashMap<String, Decision> lastDecision;
  private HashMap<String, Integer> falseNegatives;
  // JobStatistics for currently running jobs
  private HashMap<String, JobStatistics> jobNameToStatistics;
  // Initialize tasks immediately after job has been submitted.
  private HashMap<JobInProgress, AtomicInteger> assignments;

  private EagerTaskInitializationListener eagerInitListener;
  // job events listener
  private JobListener jobListener;
  // PrintWriter for logging decisions
  private PrintWriter decisionWriter;

  // constants
  private static final JobStatistics NULL_JOB_STAT = new JobStatistics("0:0:0:0");
  private static final String NULL_JOB_STAT_STR = "0:0:0:0";
  public static final String DEFAULT_JOB_NAME = "JOB";
  public static final String MAP_SFX = "_M";
  public static final String REDUCE_SFX = "_R";  
  public static final Log LOG = LogFactory.getLog(LearningScheduler.class);

  //Following variables are configurable
  private static String HISTORY_FILE_NAME = new String();
  // whether to consider multiple resources while determining overload
  private boolean MULTIPLE_RESOURCE_OVERLOAD;
  private double MIN_EXPECTED_UTILITY = 0.0;
  private double UNDERLOAD_THRESHOLD = 0.8;
  private double OVERLOAD_THRESHOLD = 1.0;
  private double PROCS_PER_CPU = 1.0;
  private int FALSE_NEGATIVE_LIMIT = 3;
  private double SUCCESS_BOOST = 2;
  //whether to distinguish between jobs
  private boolean UNIQUE_JOBS = true;
  //whether to distinguish between map and reduce task of a job
  private boolean MAP_NEQ_REDUCE = true;

  public LearningScheduler() {
    this.joblist = new ArrayList<JobInProgress>();
    lastDecision = new HashMap<String, Decision>();
    falseNegatives = new HashMap<String, Integer>();
    jobNameToStatistics = new HashMap<String, JobStatistics>();
    assignments = new HashMap<JobInProgress, AtomicInteger>();
    this.jobListener = new JobListener();
    LOG.info("Scheduler Initiated");
  }

  // scheduler configuration
  private void config() {    
    classifier = (Classifier) ReflectionUtils.newInstance(
            conf.getClass("mapred.learnsched.Classifier",
              NaiveBayesClassifier.class, Classifier.class), conf);
    
    if (classifier == null) {
      LOG.error("Error in creating classifier instance, failing back to Naive Bayes Classifier");
      classifier = new NaiveBayesClassifier();
    }
    
    utilFunc = (UtilityFunction) ReflectionUtils.newInstance(
            conf.getClass("mapred.learnsched.UtilityFunction",
              FairAssignmentUtility.class, UtilityFunction.class), conf);
    if (utilFunc == null) {
      LOG.error("Error in creating utility function instance, failing back to FairAssignmentUtility");
      utilFunc = new FairAssignmentUtility();
    }

    MIN_EXPECTED_UTILITY =
            (double) conf.getFloat("mapred.learnsched.MinExpectedUtility", 0f);
    UNDERLOAD_THRESHOLD =
           (double) conf.getFloat("mapred.learnsched.UnderloadThreshold", 0.8f);
    OVERLOAD_THRESHOLD =
           (double) conf.getFloat("mapred.learnsched.OverloadThreshold", 1.0f);
    PROCS_PER_CPU =
            (double) conf.getFloat("mapred.learnsched.ProcessesPerCpu", 1);
    FALSE_NEGATIVE_LIMIT =
            conf.getInt("mapred.learnsched.FalseNegativeLimit", 3);
    SUCCESS_BOOST = conf.getInt("mapred.learnsched.SuccessBoost", 2);
    HISTORY_FILE_NAME =
            conf.get("mapred.learnsched.HistoryFile", "decisions_%s.txt");
    MULTIPLE_RESOURCE_OVERLOAD =
            conf.getBoolean("mapred.learnsched.MultipleResources", false);
    UNIQUE_JOBS =
            conf.getBoolean("mapred.learnsched.UniqueJobs", true);
    MAP_NEQ_REDUCE =
            conf.getBoolean("mapred.learnsched.MapDifferentFromReduce", true);
  }

  @Override
  public void start() throws IOException {
    // initialize tasks immediately after job submission
    this.eagerInitListener = new EagerTaskInitializationListener(conf);
    eagerInitListener.start();
    taskTrackerManager.addJobInProgressListener(eagerInitListener);
    taskTrackerManager.addJobInProgressListener(jobListener);

    // configure the scheduler
    config();

    String dateStr = new Date().toString().replace(' ', '_').replace(':', '_');
    HISTORY_FILE_NAME = String.format(HISTORY_FILE_NAME, dateStr);

    decisionWriter = new PrintWriter(HISTORY_FILE_NAME);

    LOG.info("Will log decisions to : ".concat(HISTORY_FILE_NAME));
    LOG.info("Scheduler started");
  }

  @Override
  public void terminate() throws IOException {
    if (jobListener != null) {
      taskTrackerManager.removeJobInProgressListener(jobListener);
    }
    if (eagerInitListener != null) {
      taskTrackerManager.removeJobInProgressListener(eagerInitListener);
    }

    if (decisionWriter != null) {
      decisionWriter.close();
    }
    LOG.info("Scheduler terminated");
  }

  /**
   * Convinience method to get job name of a JobInProgress
   * @param job
   * @return Job name string
   */
  String getJobName(JobInProgress job) {
    return (UNIQUE_JOBS) ? job.getJobConf().getJobName() : DEFAULT_JOB_NAME;
  }

  /**
   * Convinience method to get job name of a TaskStatus object
   * @param task
   * @return Job name string
   */
  String getJobName(TaskStatus task) {
    return getJobName(getJobInProgress(task));
  }

  private JobStatistics getJobStatistics(JobInProgress job, boolean isMap) {
    String jobname = getJobName(job) + (isMap ? MAP_SFX : REDUCE_SFX);
    JobStatistics jobstat = jobNameToStatistics.get(jobname);
    return jobstat == null ? NULL_JOB_STAT : jobstat;
  }

  int getJobClusterID(JobInProgress job, boolean isMap) {
    String jobName = getJobName(job) + (isMap ? MAP_SFX : REDUCE_SFX);
    return jobName.hashCode();
  }

  private boolean mapsRemain(JobInProgress job) {
    return job.finishedMaps() < job.desiredMaps();
  }

  private boolean reducesRemain(JobInProgress job) {
    return job.finishedReduces() < job.desiredReduces();
  }

  /**
   * Total number of pending map tasks in this MapReduce cluster
   * @return Integer value indicating total number of pending map tasks
   */
  public int totalPendingMaps() {
    int ret = 0;
    for (JobInProgress job : joblist) {
      ret += job.pendingMaps();
    }
    return ret;
  }

  /**
   * Total number of pending reduce tasks in this MapReduce cluster
   * @return Integer value indicating total number of pending reduce tasks
   */
  public int totalPendingReduces() {
    int ret = 0;
    for (JobInProgress job : joblist) {
      ret += job.pendingReduces();
    }
    return ret;
  }

  /**
   * Convinience method to get names of jobs whose tasks are running at a TaskTracker
   * @param ttstatus
   * @return Array of strings containing names of jobs
   */
  String[] getJobNamesAtTracker(TaskTrackerStatus ttstatus) {
    List<TaskStatus> tasks = ttstatus.getTaskReports();
    String[] ret = new String[tasks.size()];
    for (int i = 0; i < ret.length; i++) {
      ret[i] = getJobName(tasks.get(i));
    }
    return ret;
  }

  /**
   * Convinience method to get JobInProgress object associated with the TaskStatus object
   * @param task
   * @return
   */
  JobInProgress getJobInProgress(TaskStatus task) {
    TaskAttemptID tid = task.getTaskID();
    JobID jobid = tid.getJobID();
    JobTracker jt = (JobTracker) taskTrackerManager;
    return jt.getJob(jobid);
  }

  private Task getNewMapTask(TaskTrackerStatus ttstatus, JobInProgress job)
          throws IOException {
    ClusterStatus clusterStatus = taskTrackerManager.getClusterStatus();
    final int numTaskTrackers = clusterStatus.getTaskTrackers();
    Task t = job.obtainNewLocalMapTask(ttstatus, numTaskTrackers,
            taskTrackerManager.getNumberOfUniqueHosts());
    if (t == null) {
      t = job.obtainNewNonLocalMapTask(ttstatus, numTaskTrackers,
              taskTrackerManager.getNumberOfUniqueHosts());
    }
    return t;
  }

  private Task getNewReduceTask(TaskTrackerStatus ttstatus, JobInProgress job)
          throws IOException {
    ClusterStatus clusterStatus = taskTrackerManager.getClusterStatus();
    final int numTaskTrackers = clusterStatus.getTaskTrackers();
    return job.obtainNewReduceTask(ttstatus, numTaskTrackers,
            taskTrackerManager.getNumberOfUniqueHosts());
  }

  // Validate an assignment decision of the task tracker
  private void validateDecision(String tracker, NodeEnvironment env) {
    Decision dd = lastDecision.get(tracker);

    if (dd == null || env == null) {
      return;
    }

    // A decision is invalid only if an assignment was made and it resulted
    // in overload on the concerned TaskTracker
    if (env.overLoaded(PROCS_PER_CPU, MULTIPLE_RESOURCE_OVERLOAD)) {
      if (dd.wasTaskAssigned()) {
        notifyResult(dd, false);
      }
    } else {
      if (!dd.wasTaskAssigned() && dd.getNodeEnv().underLoaded(PROCS_PER_CPU)) {
        // False Negative: A task was not assigned even when the node was under loaded
        // This could happen if the scheduler is learning phase, where a large number
        // of node environment states are labelled as false negatives. To counter
        // this, we keep a count of successive false negatives. While making
        // an assignment, if the number of successive false negatives is
        // greater than FALSE_NEGATIVE_LIMIT, an assignment is forcefully made,
        // ignoring the prediction of the classifier.
        Integer preCount = falseNegatives.get(tracker);
        falseNegatives.put(tracker, preCount == null ? 1 : preCount + 1);
      } else if (dd.wasTaskAssigned()) {
        falseNegatives.put(tracker, 0);
      }
      // Every success is trained multiple times to increase overall 'success'
      // probability. This is done because we get much more 'failure' samples
      // than 'success' samples.
      for (int i = 0; i < SUCCESS_BOOST; i++) {
        notifyResult(dd, true);
      }
    }

    // Log the decision
    if (decisionWriter != null) {
      decisionWriter.println(dd.toString());
      decisionWriter.flush();
    }
    lastDecision.remove(tracker);
  }

  /**
   * Update the classifier with decision result.
   * @param dd Evaluated decision
   * @param result Result of decision evaluation, true => success; false => failure
   */
  public void notifyResult(Decision dd, boolean result) {
    if (dd != null) {
      dd.setResult(result);
      if (classifier != null) {
        JobStatistics jobstat = dd.getJobStatistics();
        classifier.updateClassifier(jobstat, dd.getNodeEnv(), result);
      } else {
        LOG.warn("Unable to get classifier for job:" + dd.getSelectedJob());
      }
    }
  }

  private Decision addPendingDecision(String tracker, NodeEnvironment env,
          JobInProgress job, TaskAttemptID tid, double[] predictions,
          boolean assignTask, JobStatistics jobstat) {
    //int jobClusterID = getJobClusterID(job, tid.isMap());
    String jobName = getJobName(job);
    Decision de =
            new Decision(env, jobName, tid, predictions, assignTask);
    de.setPendingMaps(totalPendingMaps());
    de.setPendingReduces(totalPendingReduces());
    de.setJobStatistics(jobstat);
    return de;
  }

  private double[] getExepctedUtility(TaskTrackerStatus ttstatus,
          JobInProgress job, boolean isMap, NodeEnvironment env) {
    double ret[] = new double[3];
    JobStatistics jobstat = getJobStatistics(job, isMap);
    int utility = utilFunc.getUtility(this, job, isMap);
    double successDist = classifier.getSuccessDistance(jobstat, env);
    LOG.info(getJobName(job) + (isMap ? "_map" : "_reduce") + " Utility = " + utility + " Likelihood: " + successDist);
    ret[0] = successDist * utility;
    ret[1] = utility;
    ret[2] = successDist;
    return ret;
  }

  @Override
  public List<Task> assignTasks(TaskTrackerStatus ttstatus) throws IOException {
    // the environment vector
    NodeEnvironment env = new NodeEnvironment(ttstatus, this);
    ClusterStatus clusterStatus = taskTrackerManager.getClusterStatus();
    final int numTaskTrackers = clusterStatus.getTaskTrackers();
    final int numTasks = ttstatus.countMapTasks() + ttstatus.countReduceTasks();
    String trackerName = ttstatus.getTrackerName();

    // validate last decision for this tracker
    validateDecision(trackerName, env);
    // For evaluation only
    System.out.println(System.currentTimeMillis() / 1000 + "\t" +
            trackerName + "\t" +
            env.toString());
    // don't allocate tasks if node is heavily loaded
    if (env.overLoaded(PROCS_PER_CPU * OVERLOAD_THRESHOLD, MULTIPLE_RESOURCE_OVERLOAD)) {
      LOG.info(trackerName + " >>> NOT ALLOCATING BECAUSE OVERLOAD");
      return null;
    }

    JobInProgress selectedJob = null;
    double maxUtility = Double.NEGATIVE_INFINITY;
    boolean chooseMap = true;
    double maxUtilArray[] = {0, 0, 0};
    double tmpUtilArray[] = {0, 0, 0};

    List<JobInProgress> runningJobs = getRunningJobs();
    // Shuffle the list so that order of job submission does not affect
    // the task assignment decision. Any such order, if desired, must be
    // enforced by the utility function. We are shuffling a copy of the original
    // jobs list.
    Collections.shuffle(runningJobs);

    for (JobInProgress job : runningJobs) {
      double tmpUtilM[] = getExepctedUtility(ttstatus, job, true, env);
      double expectedUtilM = tmpUtilM[0];
      LOG.info("E.U. of Map tasks " + getJobName(job) + " = " + expectedUtilM);
      double tmpUtilR[] = {Double.NEGATIVE_INFINITY, 0, 0};
      double expectedUtilR = Double.NEGATIVE_INFINITY;
      if (job.desiredReduces() > 0) {
        // Get EU of reduce tasks only if the job has a reduce task.
        tmpUtilR = getExepctedUtility(ttstatus, job, false, env);
        expectedUtilR = tmpUtilR[0];
        LOG.info("E.U. of Reduce tasks " + getJobName(job) + " = " + expectedUtilR);
      }

      double expectedUtility = 0;
      boolean lChooseMap = true;
      // decide whether to allocate maps or reduces
      if (lChooseMap = (expectedUtilM > expectedUtilR)) {
        // maps have more utility, choose maps
        expectedUtility = expectedUtilM;
        tmpUtilArray = tmpUtilM;
      } else {
        // reduces have more utility, choose reduces
        expectedUtility = expectedUtilR;
        tmpUtilArray = tmpUtilR;
      }

      if (expectedUtility > maxUtility) {
        maxUtility = expectedUtility;
        selectedJob = job;
        maxUtilArray = tmpUtilArray;
        chooseMap = lChooseMap;
      }
    }

    // we do not have any jobs in the queue
    if (selectedJob == null) {
      LOG.info("No jobs");
      return null;
    }

    Task task = null;
    boolean allocateTask = false;

    // task allocation will cause overload if max expected utility is negative
    // tasks with negative expected utility are not assigned.
    if (maxUtilArray[0] > MIN_EXPECTED_UTILITY) {
      allocateTask = true;
    } else if (maxUtilArray[0] < MIN_EXPECTED_UTILITY) {
      LOG.info("##### None of the jobs have more than min utility");
      // if the node is underloaded, assign a task anyway
      boolean veryLowLoad = env.underLoaded(PROCS_PER_CPU * UNDERLOAD_THRESHOLD);
      // check if the node is not in a 'false negative loop'
      // A node is said to be in a false negative loop if no tasks have been
      // assigned to the node in last FALSE_NEGATIVE_LIMIT number of heaertbeats,
      // while the node being underloaded this entier time.
      Integer falseNegativeCount = falseNegatives.get(trackerName);
      int fnc = (falseNegativeCount == null) ? 0 : falseNegativeCount.intValue();
      // we are in false negative loop only if tasks are not being allocated.
      boolean inFalseNegativeLoop = fnc >= FALSE_NEGATIVE_LIMIT;
      allocateTask = (veryLowLoad || inFalseNegativeLoop) && (numTasks == 0);

      if (allocateTask) {
        LOG.info("---->>>> Allocating task as node underloaded? " + veryLowLoad +
                ", inFalseNegativeLoop? " + inFalseNegativeLoop);
      }
    }

    JobStatistics jobstat = null;

    if (allocateTask) {
      task = chooseMap ? getNewMapTask(ttstatus, selectedJob) : getNewReduceTask(ttstatus, selectedJob);
      if (selectedJob != null) {
        jobstat = getJobStatistics(selectedJob, chooseMap);
      }
    } else {
      jobstat = NULL_JOB_STAT;
    }

    TaskAttemptID tid = allocateTask && (task != null) ? task.getTaskID() : null;
    // Record the decision for this tracker. This decision will be evaluated when
    // next heartbeat from the tracker is received
    Decision dd = addPendingDecision(trackerName, env, selectedJob,
            tid, maxUtilArray, allocateTask, jobstat);
    lastDecision.put(trackerName, dd);

    ArrayList<Task> chosenTasks = new ArrayList<Task>(1);

    if (allocateTask && task != null) {
      chosenTasks.add(task);
      // Increment assignment count for the job
      assignments.get(selectedJob).incrementAndGet();      
      return chosenTasks;
    } else {
      return null;
    }
  }

  List<JobInProgress> getRunningJobs() {
    List<JobInProgress> rjobs = new ArrayList<JobInProgress>();
    for (JobInProgress job : joblist) {
      if (job.getStatus().getRunState() == JobStatus.RUNNING) {
        rjobs.add(job);
      }
    }
    return rjobs;
  }

  @Override
  public Collection<JobInProgress> getJobs(String arg0) {
    return joblist;
  }

  class JobListener extends JobInProgressListener {

    @Override
    public void jobAdded(JobInProgress job) {
      joblist.add(job);
      assignments.put(job, new AtomicInteger());

      String statStrMap = job.getJobConf().get("learnsched.jobstat.map", NULL_JOB_STAT_STR );
      String jobname = getJobName(job);
      if (statStrMap != null) {
        JobStatistics jobstat = new JobStatistics(statStrMap);
        String jobNameMap = jobname + MAP_SFX;
        if (!jobNameToStatistics.containsKey(jobNameMap)) {
          jobNameToStatistics.put(jobNameMap, jobstat);
        }
      } 
      
      String statStrReduce = job.getJobConf().get("learnsched.jobstat.reduce", NULL_JOB_STAT_STR);
      if (statStrReduce != null && job.desiredReduces() > 0) {
        JobStatistics jobstat = new JobStatistics(statStrReduce);
        String jobNameReduce = jobname + REDUCE_SFX;
        if (!jobNameToStatistics.containsKey(jobNameReduce)) {
          jobNameToStatistics.put(jobNameReduce, jobstat);
        }
      }
    }

    @Override
    public void jobRemoved(JobInProgress job) {
      joblist.remove(job);
      assignments.remove(job);
    }

    @Override
    public void jobUpdated(JobChangeEvent job) { /* do nothing */ }
  }

  /** Utility function that tries to achieve fairness by 
   * maximimizing utility of tasks that have the least number of task
   * assignments in a given time interval. JobPriority is also taken into 
   * consideration while calculating utility. Utility of a job is
   * given by 2^(K - jop.priority - job.assignments).
   */
  class FairAssignmentUtility implements UtilityFunction {
    Timer assignmentRefresher;
    public FairAssignmentUtility() {
      assignmentRefresher = new Timer("Job Assignment Refresher", true);
      TimerTask refresherTask = new TimerTask() {
        public void run() {
          for(Map.Entry<JobInProgress, AtomicInteger> e : assignments.entrySet()) {
            e.getValue().set(0);
          }
        }
      };
      assignmentRefresher.schedule(refresherTask, MRConstants.HEARTBEAT_INTERVAL_MIN,
              MRConstants.HEARTBEAT_INTERVAL_MIN/2);
    }
    
    public int getUtility(LearningScheduler sched, JobInProgress jip, boolean isMap) {
      int priority  = jip.getPriority().ordinal();
      AtomicInteger asgn = assignments.get(jip);
      if (asgn == null) return 0;
      return (int)Math.pow(2, 64 - priority - asgn.get());
    }
  }

}

package org.apache.hadoop.mapred;

import java.sql.SQLException;
import java.sql.Statement;

public class Decision {
  private NodeEnvironment env;
  private int jobClusterID;
  private String selectedJob;
  private boolean result;
  private boolean prediction;
  private boolean pending;
  private boolean taskAssigned;
  private boolean isMap;
  private long when;
  private double successDist;
  private int util;
  private int pendingMaps;
  private int pendingReduces;
  private double expectedUtil;
  private TaskAttemptID tid;
  private String trackerName;
  private JobStatistics jobstat;

  Decision(NodeEnvironment env, String jobName, TaskAttemptID tid, 
      double[] predictions, boolean assignTask) {
    this.env = env;
    //this.jobClusterID = jobClusterID;
    this.selectedJob = jobName;
    this.tid = tid;
    this.isMap = (tid != null) ? tid.isMap() : true;
    this.taskAssigned = assignTask;
    this.successDist = predictions[2];
    this.prediction = successDist > 0 ;
    this.util = (int) predictions[1];
    this.expectedUtil = predictions[0];    
    pending = true;
    when = System.currentTimeMillis();
    trackerName = env.trackerName;
  }
  
  String getSelectedJob() { return selectedJob; }
  boolean getIsMap() { return isMap; }
  int getJobClusterID() { return jobClusterID; }
  
  boolean wasTaskAssigned() { return taskAssigned; }
  
  NodeEnvironment getNodeEnv() { return env; }
  
  void setResult(boolean result) {
    this.result = result;
  }
  
  void setPendingMaps(int maps) { pendingMaps = maps; }
  void setPendingReduces(int reduces) { pendingReduces = reduces ;}   
  void setJobStatistics(JobStatistics jobstat) { this.jobstat = jobstat;}
  JobStatistics getJobStatistics() { return jobstat; }
 
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(this.trackerName); builder.append('\t');
    builder.append(this.selectedJob.replace(' ', '_'));    
    builder.append(this.isMap ? LearningScheduler.MAP_SFX : LearningScheduler.REDUCE_SFX);
    builder.append('\t');
    //builder.append("cid" + jobClusterID); builder.append('\t');
    builder.append(this.prediction); builder.append('\t');
    builder.append(this.result); builder.append('\t');
    builder.append(this.successDist); builder.append('\t');
    builder.append(this.util); builder.append('\t');
    builder.append(this.when); builder.append('\t');
    builder.append(this.jobstat.toString()); builder.append('\t');
    builder.append(this.env.toString());    
    return builder.toString();
  }
 
}

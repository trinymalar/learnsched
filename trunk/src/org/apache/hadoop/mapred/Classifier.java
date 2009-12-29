package org.apache.hadoop.mapred;

public interface Classifier {

  public double getSuccessDistance(JobStatistics jobstat, NodeEnvironment env);

  public void updateClassifier(JobStatistics jobstat, NodeEnvironment env, boolean result);
}

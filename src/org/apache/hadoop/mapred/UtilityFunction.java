/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.mapred;


public interface UtilityFunction {
  public int getUtility(LearningScheduler sched, JobInProgress jip, boolean isMap);
}

class FifoUtility implements UtilityFunction {
  public int getUtility(LearningScheduler sched, JobInProgress jip, boolean isMap) {
    long now = System.currentTimeMillis();
    long start = jip.getStartTime();
    long age = (now - start);    
    boolean mapsNeeded = jip.desiredMaps() > jip.finishedMaps();    
    if(isMap) {
      return mapsNeeded ? (int)(age) : 0;
    } else {
      return mapsNeeded ? 0 : (int)(age); // -ve if maps are remaining, +ve when not
    }
  }
}


class ConstantUtility implements UtilityFunction {
  public static final int UTILITY = 1000;
  public int getUtility(LearningScheduler sched, JobInProgress jip,
      boolean isMap) {
    boolean mapsNeeded = jip.desiredMaps() > jip.finishedMaps();    
    if(isMap) {
      return mapsNeeded ? UTILITY : 0 ; 
    } else {
      return mapsNeeded ? 0 : UTILITY; 
    }
  }
}

class ShortestJobFirst implements UtilityFunction {
  public static final double UTILITY = 1000000;
  public int getUtility(LearningScheduler sched, JobInProgress jip,
      boolean isMap) {
    boolean mapsNeeded = jip.desiredMaps() > jip.finishedMaps();    
    int remainingTasks = jip.pendingMaps() + jip.pendingReduces();
    double util = remainingTasks == 0 ?  0 : UTILITY / remainingTasks ;
    if(isMap) {
      return mapsNeeded ? (int) util : 0; 
    } else {
      return mapsNeeded ? 0 : (int) util; 
    }
  }
}

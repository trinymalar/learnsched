/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
    if (isMap) {
      return mapsNeeded ? (int) (age) : 0;
    } else {
      return mapsNeeded ? 0 : (int) (age); // -ve if maps are remaining, +ve when not
    }
  }
}

class ConstantUtility implements UtilityFunction {

  public static final int UTILITY = 1000;

  public int getUtility(LearningScheduler sched, JobInProgress jip,
          boolean isMap) {
    boolean mapsNeeded = jip.desiredMaps() > jip.finishedMaps();
    if (isMap) {
      return mapsNeeded ? UTILITY : 0;
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
    double util = remainingTasks == 0 ? 0 : UTILITY / remainingTasks;
    if (isMap) {
      return mapsNeeded ? (int) util : 0;
    } else {
      return mapsNeeded ? 0 : (int) util;
    }
  }
}

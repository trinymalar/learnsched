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

class NodeEnvironment {

  double loadAverage;
  long memUsed;
  long numMaps, numReduces, numCpus, memTotal, cpuFreq;
  String trackerName;
  public static final int loadScaleFactor = 10;
  double ucpu;
  
  public static final double DISKIO_LIMIT = 15;
  public static final double NETIO_LIMIT = 25;
  private static double ALPH = 0.75;

  NodeEnvironment(TaskTrackerStatus ttstatus, LearningScheduler sched) {
    TaskTrackerStatus.ResourceStatus resources = ttstatus.getResourceStatus();
    //loadAverage = resources.getLoadAverage();
    numMaps = ttstatus.countMapTasks();
    numReduces = ttstatus.countReduceTasks();
    numCpus = resources.getNumProcessors();
    memTotal = resources.getTotalPhysicalMemory();
    memUsed = memTotal - resources.getAvailablePhysicalMemory();
    cpuFreq = resources.getCpuFrequency()/500000; // KHz to 0.5 GHz
    trackerName = ttstatus.getTrackerName();
    ucpu = resources.getCpuUsage();
  }

  void update(TaskTrackerStatus ttstatus) {
      TaskTrackerStatus.ResourceStatus resources = ttstatus.getResourceStatus();
    //loadAverage = resources.getLoadAverage();
    numMaps = ttstatus.countMapTasks();
    numReduces = ttstatus.countReduceTasks();
    numCpus = resources.getNumProcessors();
    memTotal = resources.getTotalPhysicalMemory();
    memUsed = memTotal - resources.getAvailablePhysicalMemory();
    cpuFreq = resources.getCpuFrequency()/1000000;
    trackerName = ttstatus.getTrackerName();
    ucpu = ALPH * ucpu  + (1 - ALPH) * resources.getCpuUsage();
  }

  public NodeEnvironment() {
  }

  public double getLoadAverage() {
    return loadAverage;
  }

  public boolean underLoaded(double procsPerCpu) {
    return !overLoaded(procsPerCpu, true);
  }

  public String toString() {
    // use string builder
    StringBuilder builder = new StringBuilder();
    builder.append("load:"+loadAverage);
    builder.append('\t');
    builder.append("numMaps:" + this.numMaps);
    builder.append('\t');
    builder.append("numReduces:" + this.numReduces);
    builder.append('\t');
    builder.append("memUsed:" + this.memUsed);
    builder.append('\t');
    builder.append("usedCPU:" + this.ucpu);
    builder.append('\t');
    builder.append("numCPUs:" + this.numCpus);
    builder.append('\t');
    builder.append("cpuFreq:" + this.cpuFreq);
    builder.append('\t');
    builder.append("memTotal:" + this.memTotal);
    builder.append('\t');
    return builder.toString();
  }

  public boolean overLoaded(double procsPerCpu, boolean multipleResources) {
    if (multipleResources) {
      return multipleResourcesOverloaded(procsPerCpu);
    } else {
      return Math.ceil(loadAverage * loadScaleFactor) >
              (procsPerCpu * numCpus * loadScaleFactor);
    }
  }

  public boolean multipleResourcesOverloaded(double procsPerCpu) {
    boolean loadAvgOverload = Math.ceil(loadAverage) > (procsPerCpu * numCpus);
    boolean ucpuOverload = ucpu > LearningScheduler.UCPU_LIMIT;
    return loadAvgOverload && ucpuOverload;
  }
}

package org.apache.hadoop.mapred;

class NodeEnvironment {

  double loadAverage;
  long memUsed;
  long numMaps, numReduces, numCpus, memTotal, cpuFreq;
  String trackerName;
  public static final int loadScaleFactor = 10;
  double ucpu;
  public static final double UCPU_LIMIT = 95;
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
    cpuFreq = resources.getCpuFrequency();
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
    cpuFreq = resources.getCpuFrequency();
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
    boolean ucpuOverload = ucpu > UCPU_LIMIT;   
    return loadAvgOverload && ucpuOverload;
  }
}
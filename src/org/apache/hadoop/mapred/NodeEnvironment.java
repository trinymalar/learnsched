package org.apache.hadoop.mapred;

class NodeEnvironment {

  double loadAverage;
  int memUsed, swapUsed;
  int numMaps, numReduces, numCpus, memTotal, swapTotal, cpuFreq;
  String trackerName;
  public static final int loadScaleFactor = 10;
  double diskio, netio, ucpu;
  public static final double UCPU_LIMIT = 95;
  public static final double DISKIO_LIMIT = 15;
  public static final double NETIO_LIMIT = 25;

  NodeEnvironment(TaskTrackerStatus ttstatus, LearningScheduler sched) {
    TaskTrackerStatus.ResourceStatus resources = ttstatus.getResourceStatus();
    loadAverage = resources.getLoadAverage();
    numMaps = ttstatus.countMapTasks();
    numReduces = ttstatus.countReduceTasks();
    numCpus = resources.getNumCpus();
    memTotal = resources.getMemTotal();
    memUsed = memTotal - (int) resources.getMemFree();
    swapTotal = resources.getSwapTotal();
    swapUsed = swapTotal - (int) resources.getSwapFree();
    cpuFreq = resources.getCpuFreq();
    trackerName = ttstatus.getTrackerName();
    netio = resources.getNetworkIO();
    diskio = resources.getDiskIO();
    ucpu = resources.getUserCpu();
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
    builder.append(loadAverage);
    builder.append('\t');
    builder.append(this.numMaps);
    builder.append('\t');
    builder.append(this.numReduces);
    builder.append('\t');
    builder.append(this.memUsed);
    builder.append('\t');
    builder.append(this.swapUsed);
    builder.append('\t');
    builder.append(this.diskio);
    builder.append('\t');
    builder.append(this.netio);
    builder.append('\t');
    builder.append(this.ucpu);
    builder.append('\t');
    builder.append(this.numCpus);
    builder.append('\t');
    builder.append(this.cpuFreq);
    builder.append('\t');
    builder.append(this.memTotal);
    builder.append('\t');
    builder.append(this.swapTotal);
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
    boolean netioOverload = netio > NETIO_LIMIT;
    boolean diskioOverload = diskio > DISKIO_LIMIT;
    return loadAvgOverload && ucpuOverload && netioOverload && diskioOverload;
  }
}

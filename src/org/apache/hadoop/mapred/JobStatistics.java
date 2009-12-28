package org.apache.hadoop.mapred;

public class JobStatistics {
  double cpu;
  double disk;
  double net;
  double memory;

  public JobStatistics(String statStr) {
    LearningScheduler.LOG.info("New job stat string:" + statStr);
    String toks[] = statStr.split(":");
    
    /*for(String tok : toks) {
      System.out.print("["+tok+"]");
    }*/
    
    cpu = Double.parseDouble(toks[0]);
    disk = Double.parseDouble(toks[1]);
    net = Double.parseDouble(toks[2]);
    memory = Double.parseDouble(toks[3]);
  }    

  public String toString() {
    return ""+ cpu + "\t" + disk + "\t" + net + "\t" + memory;
  }
  
  public static void main(String[] args) {
    String stat = "5:5:5:5";
    JobStatistics jobstat = new JobStatistics(stat);
    System.out.println(jobstat.cpu);
  }
}

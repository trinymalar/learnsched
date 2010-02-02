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

public class JobStatistics {

  double cpu;
  double disk;
  double net;
  double memory;

  public JobStatistics(String statStr) {
    //LearningScheduler.LOG.info("New job stat string:" + statStr);
    String toks[] = statStr.split(":");

    cpu = Double.parseDouble(toks[0]);
    disk = Double.parseDouble(toks[1]);
    net = Double.parseDouble(toks[2]);
    memory = Double.parseDouble(toks[3]);
  }

  public String toString() {
    return "" + cpu + "\t" + disk + "\t" + net + "\t" + memory;
  }

  public static void main(String[] args) {
    String stat = "5:5:5:5";
    JobStatistics jobstat = new JobStatistics(stat);
    System.out.println(jobstat.cpu);
  }
}

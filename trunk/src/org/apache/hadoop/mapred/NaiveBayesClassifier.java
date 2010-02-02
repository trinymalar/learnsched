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

import java.util.concurrent.atomic.AtomicInteger;

public class NaiveBayesClassifier implements Classifier {

  private static final String featureNames[] = {"job.cpu", "job.disk", "job.net", "job.memory",
    "load", "ucpu", "memUsed", "numCpus", "cpuFreq", "numTasks"};
  //public static final Log LOG = LogFactory.getLog(NaiveBayesClassifier.class);
  public static final int SUCCESS = 0;
  public static final int FAILURE = 1;
  public static final int MIN_SAMPLES = 3;
  public static final int MAX_SAMPLES = 10000;
  public static final double SUCCESS_MARGIN = 1.0;
  private ClassifierFeature features[];
  private Histogram classProbability;
  private AtomicInteger numSamples = new AtomicInteger(0);
  private static final double ln_10_to_15 = Math.log(Math.pow(10,15));

  public NaiveBayesClassifier() {
    features = new ClassifierFeature[featureNames.length];
    for (int i = 0; i < featureNames.length; i++) {
      features[i] = new ClassifierFeature(featureNames[i], 2);
    }
    classProbability = new Histogram();
  }

  public double test(int classNum, int[] testVals) {
    double classP = 0;
    classP = classProbability.probability(classNum);
    double retVal = Math.log(classP);
    for (int i = 0; i < testVals.length; i++) {
      // multiplication of probablities may result in underflow, so to avoid that
      // we compute sum of logarithms
      retVal += Math.log(features[i].test(classNum, testVals[i]));
    }
    // We add a constant to the log likelihood in order to make it positive.
    return ln_10_to_15 + retVal;
  }

  public void train(int classNum, int[] trainVals) {
    // do not train this result if we already have lots of samples recorded.
    if (numSamples.get() > MAX_SAMPLES) {
      return;
    }
    this.numSamples.incrementAndGet();
    // update class probability
    classProbability.addValue(classNum);
    // update probabilities of individual features
    int i = 0;
    for (ClassifierFeature f : features) {
      f.train(classNum, trainVals[i++]);
    }
  }

  private int[] makeFeatures(JobStatistics jobstat, NodeEnvironment env) {
    int envFeatures[] = new int[features.length];
    int i = 0;

    envFeatures[i++] = (int) jobstat.cpu;
    envFeatures[i++] = (int) jobstat.disk;
    envFeatures[i++] = (int) jobstat.net;
    envFeatures[i++] = (int) jobstat.memory;
    envFeatures[i++] = (int) Math.ceil(env.loadAverage * NodeEnvironment.loadScaleFactor);
    envFeatures[i++] = (int) env.ucpu;
    envFeatures[i++] = (int) (env.memUsed / (1024*1204*1024));
    envFeatures[i++] = (int) env.numCpus;
    envFeatures[i++] = (int) (env.cpuFreq / 10);
    envFeatures[i++] = (int) (env.numMaps + env.numReduces);
    return envFeatures;
  }

  /**
   * Returns the log likelihood of an assignment being successful,
   * (i.e. it does not result in overload), with sign of the
   * return value indicating the class label: A positive value means a success prediction.
   * @param jobstat Job resource usage statistics
   * @param env NodeEnvironment for the concerned node
   * @return ln(10^15) + log likelihood of success
   */
  public double getSuccessDistance(JobStatistics jobstat, NodeEnvironment env) {
    // First few samples are always successful.
    if (numSamples.get() <= MIN_SAMPLES) {
      return 1.0;
    }
    // first construct features from the node environment
    int envFeatures[] = makeFeatures(jobstat, env);
    // LOG.info("Env Features" + Arrays.toString(envFeatures));
    double successDist = test(SUCCESS, envFeatures);
    double failureDist = test(FAILURE, envFeatures);
    // label failure only if failure distance > success distance by an order
    // of magnitude. Sign of the return value indicates success/failure
    double sgn = (successDist + SUCCESS_MARGIN - failureDist >= 0) ? 1 : -1;
    return sgn * successDist;
  }

  public void updateClassifier(JobStatistics jobstat, NodeEnvironment env, boolean result) {
    train(result ? SUCCESS : FAILURE, makeFeatures(jobstat, env));
  }
}

/**
 * Class for a feature variable of the classifier
 * @author meghadmin
 */
class ClassifierFeature {

  private String name;
  /**
   * Array of Histograms. There should be one Histogram per class label
   */
  private Histogram classHists[];

  public ClassifierFeature(String name, int numClasses) {
    this.name = name;
    classHists = new Histogram[numClasses];
    for (int i = 0; i < numClasses; i++) {
      classHists[i] = new Histogram();
    }
  }

  public void train(int classNum, int val) {
    Histogram hist = classHists[classNum];
    hist.addValue(val);
  }

  public double test(int classNum, int val) {
    Histogram hist = classHists[classNum];
    return hist.probability(val);
  }
}

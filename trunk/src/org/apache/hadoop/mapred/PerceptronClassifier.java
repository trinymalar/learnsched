package org.apache.hadoop.mapred;

import java.util.Random;

public class PerceptronClassifier implements Classifier {

  private double weights[];
  private static Random rand = new Random();
  private static final double B = 0.2;
  private final int dimensions;
  private static final int DEFAULT_DIMENSIONS = 10;

  public PerceptronClassifier() {
    this(DEFAULT_DIMENSIONS);
  }

  public PerceptronClassifier(int dimensions) {
    if (dimensions <= 0) {
      throw new IllegalArgumentException("dimensions should be a positive integer.");
    }
    this.dimensions = dimensions;
    // weights[0] corresponds to bias vector
    weights = new double[this.dimensions];
    /*for (int i = 0; i < weights.length; i++) {
    weights[i] = rand.nextDouble();
    }*/
  }

  private double[] makeFeatures(JobStatistics jobstat, NodeEnvironment env) {
    double envFeatures[] = new double[this.dimensions];
    int i = 0;
    envFeatures[i++] = 1; // always 1
    envFeatures[i++] = jobstat.cpu / 10;
    envFeatures[i++] = jobstat.disk / 10;
    envFeatures[i++] = jobstat.net / 10;
    envFeatures[i++] = jobstat.memory / 10;
    envFeatures[i++] = (int) Math.ceil(env.loadAverage);
    envFeatures[i++] = env.memUsed / 1024;
    envFeatures[i++] = env.swapUsed / 1024;
    //envFeatures[i++] = env.numCpus;
    //envFeatures[i++] = env.cpuFreq;
    //envFeatures[i++] = env.memTotal;
    //envFeatures[i++] = env.swapTotal;
    envFeatures[i++] = env.numMaps + env.numReduces;
    envFeatures[i++] = env.ucpu / 10;
    //envFeatures[i++] = env.diskio;
    //envFeatures[i++] = env.netio;
    for (int j = 0; j < envFeatures.length; j++) {
      envFeatures[j] = Math.ceil(envFeatures[j] * 100) / 100.0;
    }
    return envFeatures;
  }

  public double getSuccessDistance(JobStatistics jobstat, NodeEnvironment env) {
    double features[] = makeFeatures(jobstat, env);
    return dotprod(weights, features) + B;
  }

  public void updateClassifier(JobStatistics jobstat, NodeEnvironment env, boolean result) {
    double features[] = makeFeatures(jobstat, env);
    train(features, result);
  }

  private double dotprod(double v1[], double v2[]) {
    assert v1.length == v2.length;
    double dotprod = 0;
    for (int i = 0; i < v1.length; i++) {
      dotprod += v1[i] * v2[i];
    }
    return dotprod;
  }

  private void train(double input[], boolean result) {
    assert input.length == weights.length;

    double calcOutput = dotprod(weights, input);
    int sgn = 0;
    if (calcOutput < 0 && result) {
      sgn = 1;
    } else if (calcOutput >= 0 && !result) {
      sgn = -1;
    }

    for (int i = 0; i < input.length && sgn != 0; i++) {
      weights[i] += sgn * input[i] * 0.5;
    }
  }
}

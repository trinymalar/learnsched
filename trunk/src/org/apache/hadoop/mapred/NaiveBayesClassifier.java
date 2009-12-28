package org.apache.hadoop.mapred;

public class NaiveBayesClassifier implements Classifier {

  private static final String featureNames[] = {"job.cpu", "job.disk", "job.net", "job.memory",
    "load", "memUsed", "swapUsed", "numCpus",
    "cpuFreq", /*"memTotal", "swapTotal",*/ "numTasks"};
  //public static final Log LOG = LogFactory.getLog(NaiveBayesClassifier.class);
  public static final int SUCCESS = 0;
  public static final int FAILURE = 1;
  public static final int MIN_SAMPLES = 3;
  public static final int MAX_SAMPLES = 10000;
  public static final double SUCCESS_THRESHOLD = 0.9;
  private ClassifierFeature features[];
  private Histogram classProbability;
  private int numSamples;

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
    return Math.exp(retVal);
  }

  public void train(int classNum, int[] trainVals) {
    // do not train this result if we already have lots of samples recorded.
    if (numSamples > MAX_SAMPLES) {
      return;
    }
    this.numSamples++;
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
    envFeatures[i++] = env.memUsed / (1024);
    envFeatures[i++] = env.swapUsed / (1024);
    envFeatures[i++] = env.numCpus;
    envFeatures[i++] = env.cpuFreq / 10;
    //envFeatures[i++] = env.memTotal / (1024);
    //envFeatures[i++] = env.swapTotal;
    envFeatures[i++] = env.numMaps + env.numReduces;
    return envFeatures;
  }

  public double getSuccessDistance(JobStatistics jobstat, NodeEnvironment env) {
    if (numSamples <= MIN_SAMPLES) {
      return 1.0;
    }
    // first construct features from the node environment
    int envFeatures[] = makeFeatures(jobstat, env);
    // LOG.info("Env Features" + Arrays.toString(envFeatures));
    double successDist = test(SUCCESS, envFeatures);
    double failureDist = test(FAILURE, envFeatures);
    double sgn = 1;
    if (failureDist != 0) {
      if ((successDist / failureDist) < SUCCESS_THRESHOLD) {
        sgn = -1;
      }
    }
    return sgn * successDist;
  }

  public void updateClassifier(JobStatistics jobstat, NodeEnvironment env, boolean result) {
    train(result ? SUCCESS : FAILURE, makeFeatures(jobstat, env));
  }
}

class ClassifierFeature {

  private String name;
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

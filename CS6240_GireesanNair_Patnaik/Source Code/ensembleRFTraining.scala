package project

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

import org.apache.spark.mllib.classification.{ LogisticRegressionModel, LogisticRegressionWithLBFGS }

import org.apache.spark.mllib.tree.model.{ GradientBoostedTreesModel, RandomForestModel }
import org.apache.spark.mllib.tree.{ GradientBoostedTrees, RandomForest }
import org.apache.spark.mllib.tree.configuration.{ BoostingStrategy, Strategy }

/**
 * Trains 5 models of Random Forest and write it to file.
 * Validate the ensemble of models with the given validation data
 * @ args(0) : path to training data folder
 * @ args(1) : path where the model are saved
 * @ args(2) : path for validation data file
 */
object ensembleRFTraining {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("training")
    //               .setMaster("local")
    val sc = new SparkContext(conf)

    println("start trainig set   :" + System.nanoTime())

    // training Data set
    // all .csv files are picked and formed an RDD of LabeledPoint
    val trainingDataM = sc.textFile(args(0) + "/*.csv", sc.defaultParallelism)
      .map(record => record.split(","))
      .map(row => new LabeledPoint(
        row.last.toDouble,
        Vectors.dense(row.take(row.length - 1).map(str => str.toDouble))))
        
	// Balancing the input training data to equal numbers of 1 & 0
    val trainingData1 = trainingDataM.filter(lp => lp.label == 1.0)
    val count = trainingData1.count.toInt
    val trainingData0 = sc.parallelize(trainingDataM.filter(lp => lp.label == 0.0).take(count))
    
    
    val trainingData = trainingData1 ++ trainingData0

    // trainingData sample for different models are created using sampling with replacement
    // the training samples are cached
    val trainigDataM1 = trainingData.sample(true, 0.5).cache()
    val trainigDataM2 = trainingData.sample(true, 0.5).cache()
    val trainigDataM3 = trainingData.sample(true, 0.5).cache()
    val trainigDataM4 = trainingData.sample(true, 0.5).cache()
    val trainigDataM5 = trainingData.sample(true, 0.5).cache()

    println("end trainig set" + System.nanoTime())

    val modelpath = args(1)

    println("train models  :" + System.nanoTime())

    // train all 5 models and save it to the given path
    sc.parallelize(
      Array(
        trainingRF(sc, modelpath + "/RFModel1", trainigDataM1),
        trainingRF(sc, modelpath + "/RFModel2", trainigDataM2),
        trainingRF(sc, modelpath + "/RFModel3", trainigDataM3),
        trainingRF(sc, modelpath + "/RFModel4", trainigDataM4),
        trainingRF(sc, modelpath + "/RFModel5", trainigDataM5)))

    println("end training models : " + System.nanoTime())

    // load the models from modelpath
    val rfModel1 = RandomForestModel.load(sc, modelpath + "/RFModel1")
    val rfModel2 = RandomForestModel.load(sc, modelpath + "/RFModel2")
    val rfModel3 = RandomForestModel.load(sc, modelpath + "/RFModel3")
    val rfModel4 = RandomForestModel.load(sc, modelpath + "/RFModel4")
    val rfModel5 = RandomForestModel.load(sc, modelpath + "/RFModel5")

    // models are trained, validation begins

    println("validation begins : " + System.nanoTime())

    // load validation data and make it to an RDD[LabeledPoint]
    val validationData = sc.textFile(args(2))
      .map(record => record.split(","))
      .map(row => new LabeledPoint(
        row.last.toDouble,
        Vectors.dense(row.take(row.length - 1).map(str => str.toDouble))))

    // create two RDD set: one with all labels 0 and one with all label 1
    val testData1: RDD[LabeledPoint] = validationData.filter(r => r.label == 1.0).cache()
    val testData0: RDD[LabeledPoint] = validationData.filter(r => r.label == 0.0).cache()
    val test1 = testData1.count()
    val test0 = testData0.count()

    // predict for all 1, filter the wrong predictions
    var predictionAndLabels = testData1
      .filter(
        lp => calculatePrediction(lp, rfModel1, rfModel2, rfModel3, rfModel4, rfModel5) == lp.label)

    // calculate accuracy (recall) for record with label 1
    val predict1 = predictionAndLabels.count()
    println("predict count for ensemble model : " + predict1)
    println("testData count for 1 : " + test1)
    println("Accuracy for records 1 : " + predict1.toFloat / test1)

    // predict for all 0, filter the wrong predictions
    var predictionAndLabels0 = testData0
      .filter(
        lp => calculatePrediction(lp, rfModel1, rfModel2, rfModel3, rfModel4, rfModel5) == lp.label)

    // calculate accuracy for record with label 0
    val predict0 = predictionAndLabels0.count()
    println("predict count for ensemble model  : " + predict0)
    println("testData count for 0 : " + test0)
    println("Accuracy for records 1 : " + predict0.toFloat / test0)

    println("finish  :" + System.nanoTime())

    println("total Accuracy :" + (predict1 + predict0).toFloat / (test1 + test0))

  }

  /**
   * Predict the label for the given labeled point using the models given to the method
   * @ given: one label point lp, and 5 RF models
   * @ return: predicted label for given lp using the 5 RF model
   */
  def calculatePrediction(
    lp:       LabeledPoint,
    rfModel1: RandomForestModel,
    rfModel2: RandomForestModel,
    rfModel3: RandomForestModel,
    rfModel4: RandomForestModel,
    rfModel5: RandomForestModel): Double = {

    val predictions: Array[Double] =
      Array(
        predictRandomForest(lp, rfModel1),
        predictRandomForest(lp, rfModel2),
        predictRandomForest(lp, rfModel3),
        predictRandomForest(lp, rfModel4),
        predictRandomForest(lp, rfModel5))

    val sum = predictions.sum

    // predict the value based on the majority vote
    if (sum >= 2.5)
      return 1.0
    else
      return 0.0
  }

  // returns predicted label for the given labeled point using the given model
  def predictRandomForest(
    lp:      LabeledPoint,
    rfModel: RandomForestModel): Double = {
    return rfModel.predict(lp.features)

  }
  
  //returns predicted label for the given labeled point using the given model
  def predictLogisticRegression(
    lp:      LabeledPoint,
    lrModel: LogisticRegressionModel): Double = {
    return lrModel.predict(lp.features)
  }
  //returns predicted label for the given labeled point using the given model
  def predictGBT(
    lp:       LabeledPoint,
    gbtModel: GradientBoostedTreesModel): Double = {
    return gbtModel.predict(lp.features)
  }
  
  /**
   * Train the RandomForests model for the given training data
   * save the trained model in the given path
   */
  def trainingRF(sc: SparkContext, path: String, trainigDataM1: RDD[LabeledPoint]) = {
    println("training RF  :" + path + ":" + System.nanoTime())
    val treeStrategy = Strategy.defaultStrategy("Classification")
    val numTrees = 10
    val featureSubsetStrategy = "auto"
    val maxBins = 100
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val maxDepth = 5
    val impurity = "gini"
    val rfModel = RandomForest
      .trainClassifier(
        trainigDataM1,
        numClasses, categoricalFeaturesInfo, numTrees,
        featureSubsetStrategy, impurity, maxDepth, maxBins)
    rfModel.save(sc, path)
    trainigDataM1.unpersist()
    println("training RF ends : " + System.nanoTime())
  }

  /**
   * Train the GradientBoostedTrees model for the given training data
   * save the trained model in the given path
   */
  def trainingGBT(sc: SparkContext, path: String, trainigDataM2: RDD[LabeledPoint]) = {
    println("training GBT  :" + System.nanoTime())

    val numBoostingIterations = 3
    val maxBoostingDepth = 4
    val numClasses = 2

    var boostingStrategy = BoostingStrategy.defaultParams("Classification")
    boostingStrategy.setNumIterations(numBoostingIterations)
    boostingStrategy.treeStrategy.setNumClasses(numClasses)
    boostingStrategy.treeStrategy.setMaxDepth(maxBoostingDepth)

    GradientBoostedTrees.train(
      trainigDataM2,
      boostingStrategy).save(sc, path)
    trainigDataM2.unpersist()

    println("training GBT ends :" + System.nanoTime())
  }

  /**
   * Train the LogisticRegression  model for the given training data
   * save the trained model in the given path
   */
  def trainingLR(sc: SparkContext, path: String, trainigDataM3: RDD[LabeledPoint]) = {
    println("training LR :" + System.nanoTime())
    val numClasses = 2

    val lrModel = new LogisticRegressionWithLBFGS()
      .setNumClasses(numClasses)
      .run(trainigDataM3)

    lrModel.save(sc, path)
    trainigDataM3.unpersist()
    println("training LR ends :" + System.nanoTime())
  }

}
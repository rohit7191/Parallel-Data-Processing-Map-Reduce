package cs6240.project
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel

object prediction {
  def main(args: Array[String]) {
    /*
     * Setting the Spark Configuration 
     */
    val conf = new SparkConf()
      .setAppName("ModelPrediction")
    //  .setMaster("local")

    val sc = new SparkContext(conf)
    
    /*
     * testData is the Image-5 file
     * Pre-processing it and converting it to RDD[LabeledPoint]
     */
    val testData = sc.textFile(args(0))
      .map(record => record.split(","))
      .map(row => new LabeledPoint(
        0.0,
        Vectors.dense(row.take(row.length - 1).map(str => str.toDouble))))
        
    /*
     * models refers to the Saved Model path created by ensembleRFTraining    
     */
    val models = args(1)
    println("start load models")

    /*
     * Loading the 5 models
     */
    val rfModel1 = RandomForestModel.load(sc, models + "/RFModel1")
    val rfModel2 = RandomForestModel.load(sc, models + "/RFModel2")
    val rfModel3 = RandomForestModel.load(sc, models + "/RFModel3")
    val rfModel4 = RandomForestModel.load(sc, models + "/RFModel4")
    val rfModel5 = RandomForestModel.load(sc, models + "/RFModel5")

    println("here 1")

    val testDataCount = testData.count()

    println("here is my model")
    
    /*
     * Creating an Ensemble which gives us the prediction on Image-5 file
     */
    val ensembleResult: RDD[(Int)] =
      testData.map(
        lp => calculatePrediction(
          lp,
          rfModel1,
          rfModel2,
          rfModel3,
          rfModel4,
          rfModel5))
    println("end prediction")
    ensembleResult.repartition(1).saveAsTextFile(args(2))

  }

  /*
   * Calculating the Prediction for Image-5 file by passing all the models and the labeledPoint where the label has
   * been converted from '?' to 0.0
   */
  def calculatePrediction(
    lp: LabeledPoint,
    rfModel1: RandomForestModel,
    rfModel2: RandomForestModel,
    rfModel3: RandomForestModel,
    rfModel4: RandomForestModel,
    rfModel5: RandomForestModel): Int = {

    val predictions: Array[Int] =
      Array(
        predictRandomForest(lp, rfModel1).toInt,
        predictRandomForest(lp, rfModel2).toInt,
        predictRandomForest(lp, rfModel3).toInt,
        predictRandomForest(lp, rfModel4).toInt,
        predictRandomForest(lp, rfModel5).toInt)
    
    /*
     * Calculating the sum of predictions by all 5 models on a single labeled point
     * and returns the majority
     */
    val sum = predictions.sum

    if (sum >= 3)
      return 1
    else
      return 0

  }

  /*
   * Uses Random Forest Model to predict on the features of Image-5 file.
   */
  def predictRandomForest(
    lp: LabeledPoint,
    rfModel: RandomForestModel): Double = {
    return rfModel.predict(lp.features)

  }

}
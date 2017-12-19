package cs6240.hw4
import java.io.Serializable
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.TaskContext
import org.apache.spark.HashPartitioner


object SparkDriver extends Serializable {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("PageRank").setMaster("local");
    val sc = new SparkContext(conf);

    val startParseTime = System.nanoTime();

    val parserJob = new ScalaParser(sc);

    val pageAdjacencyList = parserJob.runParser(args(0)+"/*.bz2");

    val totalNumNodes = pageAdjacencyList.count();

    val initial_pagerank = 1.0 / totalNumNodes.doubleValue();
	/*
	 * Calculates initial_pagerank 
	 */
    var pageRankGraph = pageAdjacencyList
      .map(tuple => (tuple._1, (initial_pagerank, tuple._2)))
      .partitionBy(new HashPartitioner(Runtime.getRuntime().availableProcessors())).persist;
    var danglingNodeGraph = pageRankGraph
      .filter(tuple => tuple._2._2.isEmpty);
    var danglingNodePageRank = sc.doubleAccumulator
    danglingNodePageRank.add(calculateDanglingNodePageRank(danglingNodeGraph, totalNumNodes));
    val endParseTime = System.nanoTime();

    val parseTime = endParseTime - startParseTime;
    print("Parse Time:" + parseTime);

    val startPageRankTime = System.nanoTime();
    /*
     * Calculating page rank
     */
    val pageRankJob = new PageRank(sc, totalNumNodes);
    for (iteration <- 1 to 10) {
      pageRankGraph = pageRankJob.runPageRank(pageRankGraph, danglingNodePageRank.sum);
      danglingNodeGraph = pageRankGraph
        .filter(tuple => tuple._2._2.isEmpty);
      danglingNodePageRank.reset();
      danglingNodePageRank.add(calculateDanglingNodePageRank(danglingNodeGraph, totalNumNodes));
    }
    val endPageRankTime = System.nanoTime();

    val pageRankTime = endPageRankTime - startPageRankTime;
    print("Page Rank Time:" + pageRankTime);

    val startTopKTime = System.nanoTime();
	/*
	 * Top K exec
	 */
    val top100Pages = pageRankGraph
      .map(x => (x._1, x._2._1))
      .takeOrdered(100)(Ordering[Double]
        .reverse.on { (tuple: (String, Double)) => (tuple._2) });

    val endTopKTime = System.nanoTime();
    val topKTime = endTopKTime - startTopKTime;
    print("TopK Time:" + topKTime);
    sc.parallelize(top100Pages)
      .repartition(1)
      .sortBy(-_._2)
      .saveAsTextFile(args(1));

    sc.stop();
  }

  /**
   * Calculates the dangling node page rank
   */
  def calculateDanglingNodePageRank(danglingNodeGraph: RDD[(String, (Double, List[String]))], totalNumNodes: Long): Double = {

    val danglingContribution = danglingNodeGraph
      .aggregate(0.0: Double)(
        (danglingAccumulator: Double, tuple: (String, (Double, List[String]))) => (danglingAccumulator + tuple._2._1),
        (danglingAccumulator1: Double, danglingAccumulator2: Double) =>
          (danglingAccumulator1 + danglingAccumulator2));
    return danglingContribution / totalNumNodes.doubleValue();
  }

}
package cs6240.hw4
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext
import java.io.Serializable
import org.apache.spark.rdd.RDD


class PageRank(sc: SparkContext, totalNumNodes: Long) extends Serializable {
			/*
			 * Calculating the Page Rank value of the page
			 */	
  def runPageRank(pageRankGraph: RDD[(String, (Double, List[String]))], danglingNodePageRank: Double): RDD[(String, (Double, List[String]))] = {

    val outPageRankVal = pageRankGraph
      .flatMap { (tuple: (String, (Double, List[String]))) =>

        val pageRankNode = (tuple._1, (0.0, tuple._2._2));

        val numOfOutlinks = tuple._2._2.size;
        val pageRank = tuple._2._1;
		
			/*
			 * Calculating the deltaPageRank
			 */
        val deltaPageRank = ((1 - 0.15) * danglingNodePageRank / totalNumNodes.doubleValue());
		/*
			 * Calculating the new PageRank
			 */
        val  newPageRank = pageRank + deltaPageRank;
        val pageRankOutVal = (outlink: String) =>
          (outlink, ((newPageRank / numOfOutlinks.doubleValue()), List[String]()));
        tuple._2._2.map(pageRankOutVal).union(List(pageRankNode));
      }
   
    outPageRankVal
      .reduceByKey((tuple1: (Double, List[String]), tuple2: (Double, List[String])) => (
        tuple1._1 + tuple2._1, tuple1._2.union(tuple2._2)))
      .mapValues((tuple: (Double, List[String])) =>
        (((0.15 / totalNumNodes.doubleValue()) + ((1 - 0.15) * (danglingNodePageRank + tuple._1))), tuple._2));
  }
}
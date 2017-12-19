package cs6240.hw4
import java.io.Serializable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class ScalaParser(sc: SparkContext) extends Serializable {
/*
 * Parses the bz files
 */
  def runParser(input: String): RDD[(String, List[String])] = {

    val inputLines = sc.textFile(input);
    val parsedLines = inputLines
      .map(line => Bz2WikiParser.parseLine(line))
      .filter(line => line.length >= 1)
      .flatMap(line => line.split("\\n"));

    val graph = parsedLines
      .map(createGraph)
      .reduceByKey((a, b) => (a ++ b));
    return graph;
  }

/*
 * Creates adjacencyList
 */
 
  def createGraph = {
    (parsedLine: String) =>

      val splitArr = parsedLine.split(":" + ":");
      val pageName = splitArr(0);
      if (splitArr.size == 2) {

        val adjacencyList = splitArr(1).split(":").toList;
        (pageName, adjacencyList);

      } else {
        (pageName, List[String]());
      }
  }
}
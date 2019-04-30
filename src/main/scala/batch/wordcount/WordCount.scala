package batch.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(
      new SparkConf()
        .setMaster("local")
        .setAppName("WordCount")
    )

    val textFile = sc.textFile("files/wordcount/in.txt")

    val counts = textFile
      .map(_.toLowerCase)
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    counts.foreach(println)

    sc.stop()
  }

}

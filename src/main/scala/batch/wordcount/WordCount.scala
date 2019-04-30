package batch.wordcount

import org.apache.spark.{SparkConf, SparkContext}

/**
  * First example: reads words from a file and counts the occurrence of each word.
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val master = if (args.length > 0) args(0) else "local"
    val filePath = if (args.length > 1) args(1) else "./"

    val sc = new SparkContext(
      new SparkConf()
        .setMaster(master)
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

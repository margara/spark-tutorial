package batch.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf()
    config.setAppName("WordCount")
    config.setMaster("local")
    val sc = new SparkContext(config)

    val textFile = sc.textFile("/Users/margara/Desktop/in.txt")

    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    counts.saveAsTextFile("/Users/margara/Desktop/out.txt")

    sc.stop()
  }

}

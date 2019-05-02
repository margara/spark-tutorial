package streaming.wordcount

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import utils.Utils

/**
  * First streaming example:
  * reads words from a socket and counts the occurrence of each word in a window of 10 seconds
  * sliding every 5 seconds.
  *
  * Before running it, you need to open a socket on the defined host and address
  * (by default localhost:9999). For example, you can run nc -l 9999 on the terminal.
  */
object StreamingWordCount {
  def main(args: Array[String]): Unit = {
    Utils.setLogLevels()

    // You need to use at least two threads, one reads from the socket and one computes and prints
    val master = if (args.length > 0) args(0) else "local[2]"
    val socketHost = if (args.length > 1) args(1) else "localhost"
    val socketPort = if (args.length > 2) args(2).toInt else 9999

    val sc = new StreamingContext(
      new SparkConf()
        .setMaster(master)
        .setAppName("StreamingWordCount"),
      Seconds(1)
    )

    val textLines = sc.socketTextStream(socketHost, socketPort)

    val counts = textLines
      .window(Seconds(10), Seconds(5))
      .map(_.toLowerCase)
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    counts.foreachRDD(rdd => rdd.collect().foreach(println))

    sc.start()
    sc.awaitTermination()
  }

}

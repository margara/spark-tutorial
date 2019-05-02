package streaming.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import utils.Utils

/**
  * Stateful streaming example:
  * reads words from a socket and counts the occurrence of each word, printing the new
  * results every 5 seconds.
  *
  * Before running it, you need to open a socket on the defined host and address
  * (by default localhost:9999). For example, you can run nc -l 9999 on the terminal.
  */
object StreamingWordCountWithState {
  def main(args: Array[String]): Unit = {
    Utils.setLogLevels()

    // You need to use at least two threads, one reads from the socket and one computes and prints
    val master = if (args.length > 0) args(0) else "local[2]"
    val socketHost = if (args.length > 1) args(1) else "localhost"
    val socketPort = if (args.length > 2) args(2).toInt else 9999

    val sc = new StreamingContext(
      new SparkConf()
        .setMaster(master)
        .setAppName("StreamingWordCountWithState"),
      Seconds(5)
    )

    // Checkpoint directory where the state is saved
    sc.checkpoint("/tmp/")

    val textLines = sc.socketTextStream(socketHost, socketPort)

    val initialRDD = sc.sparkContext.emptyRDD[Tuple2[String, Int]]

    val stateMapFunction = (word: String, count: Option[Int], state: State[Int]) => {
      val sum = count.getOrElse(0) + state.getOption.getOrElse(0)
      val output = (word, sum)
      state.update(sum)
      output
    }

    val counts = textLines
      .window(Seconds(5))
      .map(_.toLowerCase)
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .mapWithState(StateSpec.function(stateMapFunction).initialState(initialRDD))

    counts.foreachRDD(rdd => rdd.collect().foreach(println))

    sc.start()
    sc.awaitTermination()
  }

}

package streaming.wordcount


import org.apache.spark.sql.SparkSession
import utils.Utils

/**
  * Structured streaming example:
  * reads words from a socket and counts the occurrence of each word, printing the new
  * results as they become available.
  *
  * Before running it, you need to open a socket on the defined host and address
  * (by default localhost:9999). For example, you can run nc -l 9999 on the terminal.
  */
object StructuredStreamingWordCount {
  def main(args: Array[String]): Unit = {
    Utils.setLogLevels()

    // You need to use at least two threads, one reads from the socket and one computes and prints
    val master = if (args.length > 0) args(0) else "local[4]"
    val socketHost = if (args.length > 1) args(1) else "localhost"
    val socketPort = if (args.length > 2) args(2).toInt else 9999

    val spark = SparkSession.builder
      .master(master)
      .appName("StructuredStreamingWordCount")
      .getOrCreate

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val lines = spark.readStream
      .format("socket")
      .option("host", socketHost)
      .option("port", socketPort)
      .load()

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    // Start running the query that prints the running counts to the console
    // There are three types of output
    // 1. Complete: outputs the entire result table
    // 2. Append: outputs only the new rows appended to the result table.
    //    It is applicable only when existing rows are not expected to change (so, not in this case).
    // 3. Update: outputs only the rows that were updated since the last trigger.
    val query = wordCounts.writeStream
      .outputMode("update")
      .format("console")
      .start()

    query.awaitTermination()
  }

}

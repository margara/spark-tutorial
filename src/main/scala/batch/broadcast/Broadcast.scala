package batch.broadcast

import org.apache.spark.{SparkConf, SparkContext}
import utils.Utils

/**
  * Example: using broadcast variables.
  */
object Broadcast {
  def main(args: Array[String]): Unit = {
    Utils.setLogLevels()

    val master = if (args.length > 0) args(0) else "local[4]"
    val filePath = if (args.length > 1) args(1) else "./"

    val sc = new SparkContext(
      new SparkConf()
        .setMaster(master)
        .setAppName("Broadcast")
    )

    val broadcastVar = sc.broadcast(Array("1", "2", "3"))
    println(broadcastVar.value.mkString(" "))

    val result = sc.parallelize(Array(1, 2, 3, 4))
      .map(x => x + broadcastVar.value.length)
      .collect()

    println(result.mkString(" "))

    sc.stop()
  }

}

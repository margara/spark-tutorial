package batch.closure

import org.apache.spark.{SparkConf, SparkContext}
import utils.Utils
import Array._

/**
  * Using closures: do NOT do this!
  */
object Closure {
  def main(args: Array[String]): Unit = {
    Utils.setLogLevels()

    val master = if (args.length > 0) args(0) else "local[4]"
    val filePath = if (args.length > 1) args(1) else "./"

    val sc = new SparkContext(
      new SparkConf()
        .setMaster(master)
        .setAppName("Closure")
    )

    var sum = 0;
    val input = range(1, 1000)
    sc.parallelize(input).foreach(x => sum += x)
    sc.stop()

    println(sum)


  }

}

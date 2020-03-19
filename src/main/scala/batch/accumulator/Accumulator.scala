package batch.accumulator

import org.apache.spark.{SparkConf, SparkContext}
import utils.Utils

/**
  * Example: using accumulators.
  */
object Accumulator {
  def main(args: Array[String]): Unit = {
    Utils.setLogLevels()

    val master = if (args.length > 0) args(0) else "local[4]"
    val filePath = if (args.length > 1) args(1) else "./"

    val sc = new SparkContext(
      new SparkConf()
        .setMaster(master)
        .setAppName("Accumulator")
    )

    val accum = sc.longAccumulator("My Accumulator")
    println(accum.value)

    sc.parallelize(Array(1, 2, 3, 4)).foreach(x => accum.add(x))
    println(accum.value)

    sc.stop()
  }

}

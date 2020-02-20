package batch.iterative.local

import org.apache.spark.{SparkConf, SparkContext}
import utils.Utils

/**
  * Iterative example: sum the length of word for each line.
  * Version 3: with persist() and unpersist().
  * Wrong version, due to a concurrency problem between the driver and the executor inside the while loop.
  */
object LocalIterativeSum3 {
  def main(args: Array[String]): Unit = {
    Utils.setLogLevels()

    val master = if (args.length > 0) args(0) else "local[4]"
    val filePath = if (args.length > 1) args(1) else "./"

    val sc = new SparkContext(
      new SparkConf()
        .setMaster(master)
        .setAppName("LocalIterativeSum3")
    )

    val textFile = sc.textFile(filePath + "files/iterativeSum/single_line.txt")

    // Transforms each line into a tuple (words, index, sum)
    // index is the index of the word to consider at the current iteration
    // sum is the partial sum of words at the current iteration
    var partialResult = textFile.map(w => (w.split(" "), 0, 0))
    partialResult.persist()
    var stillToProcess = partialResult.filter(r => r._1.length > r._2)

    while (! stillToProcess.isEmpty()) {
      print("*** Iteration ***\n")
      val newPartialResult = partialResult.map(w => {
        print("Map\n")
        if (w._1.length > w._2) (w._1, w._2+1, w._3 + w._1(w._2).length)
        else w
      })
      partialResult.unpersist()
      partialResult = newPartialResult
      partialResult.persist()
      stillToProcess = partialResult.filter(r => r._1.length > r._2)
    }

    partialResult.foreach(r => println(r._1.mkString(" ") + " " + r._3))

    sc.stop()
  }

}

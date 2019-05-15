package batch.iterative

import org.apache.spark.{SparkConf, SparkContext}
import utils.Utils

/**
  * First example: reads words from a file and counts the occurrence of each word.
  */
object IterativeSum {
  def main(args: Array[String]): Unit = {
    Utils.setLogLevels()

    val master = if (args.length > 0) args(0) else "local[4]"
    val filePath = if (args.length > 1) args(1) else "./"

    val sc = new SparkContext(
      new SparkConf()
        .setMaster(master)
        .setAppName("IterativeSum")
    )

    val textFile = sc.textFile(filePath + "files/iterativeSum/in.txt")

    // Transforms each line into a tuple (words, index, sum)
    // index is the index of the word to consider at the current iteration
    // sum is the partial sum of words at the current iteration
    var partialResult = textFile.map(w => (w.split(" "), 0, 0))
    var stillToProcess = partialResult.filter(r => r._1.length > r._2)

    while (! stillToProcess.isEmpty()) {
      partialResult = partialResult.map(w => {
        if (w._1.length > w._2) (w._1, w._2+1, w._3 + w._1(w._2).length)
        else w
      })
      stillToProcess = partialResult.filter(r => r._1.length > r._2)
    }

    partialResult.foreach(r => println(r._1.mkString(" ") + " " + r._3))

    sc.stop()
  }

}
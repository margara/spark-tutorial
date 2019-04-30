package batch.bank

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.max

object Bank {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(
      new SparkConf()
        .setMaster("local")
        .setAppName("Bank")
    )

    def fromLineToTuple3(line: String): (String, String, Int) = {
      val splitLine = line.split(",")
      splitLine match {
        case Array(f1: String, f2: String, f3: String) => (f1, f2, f3.toInt)
      }
    }

    val spark = SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate

    val customSchema = StructType(
      Array(
        StructField("person", StringType, true),
        StructField("account", StringType, true),
        StructField("amount", IntegerType, true),
      )
    )

    val deposits = spark.read
      .option("header", "false")
      .option("delimiter", ",")
      .schema(customSchema)
      .csv("files/bank/deposits.csv")

    val withdrawals = spark
      .read
      .option("header", "false")
      .option("delimiter", ",")
      .schema(customSchema)
      .csv("files/bank/withdrawals.csv")

    // Person with the maximum total amount of withdrawals

    withdrawals
        .groupBy("person")
        .sum("amount")
        .drop("account")
        .agg(max("sum(amount)"))

    // Accounts with negative balance

    val totalWithdrawalsPerAccount = withdrawals
        .groupBy("account")
        .sum("amount")
        .drop("person")
        .as("totalWithdrawals")

    val totalDepositsPerAccount = deposits
      .groupBy("account")
      .sum("amount")
      .drop("person")
      .as("totalDeposits")

    val negativeAccounts = totalWithdrawalsPerAccount
      .join(totalDepositsPerAccount, Seq("account"), "left_outer")
      .filter(
        totalDepositsPerAccount("sum(amount)").isNull.and(totalWithdrawalsPerAccount("sum(amount)") > 0)
          .or(totalWithdrawalsPerAccount("sum(amount)")  > totalDepositsPerAccount("sum(amount)"))
      )
      .drop("sum(amount)")

    negativeAccounts.collect.foreach(println)

    sc.stop()
  }

}

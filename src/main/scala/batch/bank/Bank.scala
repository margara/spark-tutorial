package batch.bank

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.max

/**
  * Bank example
  *
  * Input: csv files with list of deposits and withdrawals,
  * having the following schema ("person: String, account: String, amount: Int)
  *
  * Two queries
  * Q1. Print the person with the maximum total amount of withdrawals
  * Q2. Print all the accounts with a negative balance
  *
  * The code exemplifies the use of SQL primitives
  */
object Bank {
  def main(args: Array[String]): Unit = {
    val master = if (args.length > 0) args(0) else "local"
    val filePath = if (args.length > 1) args(1) else "./"

    val sc = new SparkContext(
      new SparkConf()
        .setMaster(master)
        .setAppName("Bank")
    )

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
      .csv(filePath + "files/bank/deposits.csv")

    val withdrawals = spark
      .read
      .option("header", "false")
      .option("delimiter", ",")
      .schema(customSchema)
      .csv(filePath + "files/bank/withdrawals.csv")

    // Person with the maximum total amount of withdrawals

    withdrawals
        .groupBy("person")
        .sum("amount")
        .drop("account")
        .agg(max("sum(amount)"))

    withdrawals.collect().foreach(println)

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

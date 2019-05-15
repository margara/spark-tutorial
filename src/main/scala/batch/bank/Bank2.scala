package batch.bank

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import utils.Utils

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
  *
  * Version 2. Exemplifies the benefits of persisting intermediate results to
  * enable reuse.
  */
object Bank2 {
  def main(args: Array[String]): Unit = {
    Utils.setLogLevels()

    val master = if (args.length > 0) args(0) else "local[4]"
    val filePath = if (args.length > 1) args(1) else "./"

    val spark = SparkSession.builder
      .master(master)
      .appName("Bank2")
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

    withdrawals.persist()

    withdrawals.collect().foreach(println)

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

    spark.stop()
  }

}

package com.beta.RW
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.current_date


class Read {
  val spark_ = new SparkConnector
  val spark = spark_.getSession()

  def readData(location: String): DataFrame = {

    spark.read
      .format("csv")
      .option("header", "true")
      .option("mode", "dropmalformed")
      .option("delimiter", ";")
      .load(location)
      .withColumn("technical_partition", current_date())
  }

    def writeData(dr: DataFrame ,location: String): Unit = {

        dr.write
        .format("csv")
        .mode("overwrite")
        .option("delimiter", ";")
        .partitionBy("technical_partition")
        .save(location)

    }
  }
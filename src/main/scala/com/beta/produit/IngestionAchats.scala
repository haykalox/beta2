package com.beta.produit

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.current_date

object IngestionAchats {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("beta2")
      .config("spark.master","local")
      .enableHiveSupport()
      .getOrCreate()

    val dp = spark.read
      .format("csv")
      .option("header","true")
      .option("mode","dropmalformed")
      .option("delimiter",";")
      .load("/data/sql/achats.csv")
      .withColumn("technical_partition", current_date())


    dp.write
      .format("csv")
      .mode("overwrite")
      .option("delimiter",";")
      .partitionBy("technical_partition")
      .save("/apps/hive/external/default/achats")

     spark.sql("drop table achats")

    spark.sql(
      """CREATE EXTERNAL TABLE IF NOT EXISTS
        |achats (id_produit Int,id_clients Int,Qt int,prix_unitaire int)
        |ROW FORMAT DELIMITED
        |FIELDS TERMINATED BY ';'
        |STORED AS TEXTFILE
        |PARTITIONED BY (technical_partition date)
        |LOCATION '/apps/hive/external/default/achats'
        |""".stripMargin)
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.listStatus(new Path("/apps/hive/external/default/achats"))
      .filter(_.isDirectory)
      .map(_.getPath.getName.replaceFirst("technical_partition=",""))
      .foreach(fs =>
        spark.sql(s"""alter table achats add if not exists partition(technical_partition='$fs')"""))

    spark.sql("SELECT * FROM achats").show()
  }
}

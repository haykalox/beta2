package com.beta.produit


import com.beta.RW.{Read, SparkConnector};

object test {
  def main(args: Array[String]): Unit = {

    val Spark = new SparkConnector
    val spark = Spark.getSession()

    val dx = new Read
    val dr = dx.readData("/data/sql/test.csv")


    val data_schema = dr.schema.map(m => m.name+" "+m.dataType.typeName).mkString(", ")


/*
    spark.sql(
      s"""CREATE EXTERNAL TABLE IF NOT EXISTS
        |test ($my_schema)
        |ROW FORMAT DELIMITED
        |FIELDS TERMINATED BY ';'
        |STORED AS TEXTFILE
        |PARTITIONED BY (technical_partition date)
        |LOCATION '/apps/hive/external/default/test'
        |""".stripMargin)

spark.sql("select * from test")

*/


}}

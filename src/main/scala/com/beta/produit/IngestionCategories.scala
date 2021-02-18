package com.beta.produit

import com.beta.RW.{Read, SparkConnector}

object IngestionCategories {
  def main(args: Array[String]): Unit = {

    val Spark = new SparkConnector
    val spark = Spark.getSession()

    val dx = new Read
    val dr = dx.readData("/data/sql/categories.csv")
    val dw = dx.writeData(dr,"/apps/hive/external/default/categories/","categories")
/*
    spark.sql("drop table if EXISTS categories")

    spark.sql(
      """CREATE EXTERNAL TABLE IF NOT EXISTS
        |categories (id_categorie Int,nom String)
        |ROW FORMAT DELIMITED
        |FIELDS TERMINATED BY ';'
        |STORED AS TEXTFILE
        |PARTITIONED BY (technical_partition date)
        |LOCATION '/apps/hive/external/default/categories'
        |""".stripMargin)

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.listStatus(new Path("/apps/hive/external/default/categories"))
      .filter(_.isDirectory)
      .map(_.getPath.getName.replaceFirst("technical_partition=",""))
      .foreach(fs =>
        spark.sql(s"""alter table categories add if not exists partition(technical_partition='$fs')"""))
  */
    spark.sql("SELECT * FROM categories").show()




  }
}

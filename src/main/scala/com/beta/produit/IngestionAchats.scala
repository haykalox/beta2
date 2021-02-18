package com.beta.produit

import com.beta.RW.{Read, SparkConnector}

object IngestionAchats {
  def main(args: Array[String]): Unit = {

    val Spark = new SparkConnector
    val spark = Spark.getSession()

    val dx = new Read
    val dr = dx.readData("/data/sql/achats.csv")
    val dw = dx.writeData(dr,"/apps/hive/external/default/achats/","achats")
/*
     spark.sql("drop table if EXISTS achats")

    spark.sql(
      """CREATE EXTERNAL TABLE IF NOT EXISTS
        |achats (id_produit Int,id_client Int,Qt int,prix_unitaire int)
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
*/
    spark.sql("SELECT * FROM achats").show()

  }
}

package com.beta.produit

import com.beta.RW.{Read, SparkConnector}
import org.apache.hadoop.fs.{FileSystem, Path}

object IngestionClients {
  def main(args: Array[String]): Unit = {

    val Spark = new SparkConnector
    val spark = Spark.getSession()

    val dx = new Read
    val dr = dx.readData("/data/sql/clients.csv")
    val dw = dx.writeData(dr,"/apps/hive/external/default/clients/")

 spark.sql("drop table clients")

    spark.sql(
      """CREATE EXTERNAL TABLE IF NOT EXISTS
        |clients (id_Client int,nom string)
        |ROW FORMAT DELIMITED
        |FIELDS TERMINATED BY ';'
        |STORED AS TEXTFILE
        |PARTITIONED BY (technical_partition date)
        |LOCATION '/apps/hive/external/default/clients'
        |""".stripMargin)
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.listStatus(new Path("/apps/hive/external/default/clients"))
      .filter(_.isDirectory)
      .map(_.getPath.getName.replaceFirst("technical_partition=",""))
      .foreach(fs =>
        spark.sql(s"""alter table clients add if not exists partition(technical_partition='$fs')"""))
    spark.sql("SELECT * FROM clients").show()

  }
}

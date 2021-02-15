package com.beta.produit
import com.beta.RW._
import org.apache.hadoop.fs.{FileSystem, Path}

object IngestionProduits {
  def main(args: Array[String]): Unit = {

    val Spark = new SparkConnector
    val spark = Spark.getSession()

    val dx = new Read
    val dr = dx.readData("/data/sql/produits.csv")
    val dw = dx.writeData(dr,"/apps/hive/external/default/produits/")

    spark.sql("drop table produits")

    spark.sql(
      """CREATE EXTERNAL TABLE IF NOT EXISTS
        |produits (id_produit Int,id_categorie Int,nom String)
        |ROW FORMAT DELIMITED
        |FIELDS TERMINATED BY ';'
        |STORED AS TEXTFILE
        |PARTITIONED BY (technical_partition date)
        |LOCATION '/apps/hive/external/default/produits/'
        |""".stripMargin)

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.listStatus(new Path("/apps/hive/external/default/produits"))
      .filter(_.isDirectory)
      .map(_.getPath.getName.replaceFirst("technical_partition=",""))
      .foreach(fs =>
        spark.sql(s"""alter table produits add if not exists partition(technical_partition='$fs')"""))
    spark.sql("SELECT * FROM produits").show()

}}

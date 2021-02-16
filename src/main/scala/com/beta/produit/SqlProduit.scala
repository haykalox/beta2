package com.beta.produit

import com.beta.RW.{Read, SparkConnector}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.current_date

object SqlProduit {
  def main(args: Array[String]): Unit = {


    val spark_ = new SparkConnector
    val spark = spark_.getSession()

    val df = spark.sql("select produits.id_produit,nom,achats.qt from produits INNER JOIN (select id_produit,SUM(qt) as qt from achats group by id_produit order by qt asc limit 5)achats ON produits.id_produit=achats.id_produit ")
      .withColumn("technical_partition", current_date())

    val dx = new Read
    val dw = dx.writeData(df,"/apps/hive/external/default/achat_produit")

    spark.sql("drop table if EXISTS achat_produit")

    spark.sql(
      """CREATE EXTERNAL TABLE IF NOT EXISTS
        |achat_produit (id_produit Int,nom String,quantite_achat Int)
        |ROW FORMAT DELIMITED
        |FIELDS TERMINATED BY ';'
        |STORED AS TEXTFILE
        |PARTITIONED BY (technical_partition date)
        |LOCATION '/apps/hive/external/default/achat_produit/'
        |""".stripMargin)

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.listStatus(new Path("/apps/hive/external/default/achat_produit"))
      .filter(_.isDirectory)
      .map(_.getPath.getName.replaceFirst("technical_partition=",""))
      .foreach(fs =>
        spark.sql(s"""alter table achat_produit add if not exists partition(technical_partition='$fs')"""))
    spark.sql("SELECT * FROM achat_produit").show()

  }
}

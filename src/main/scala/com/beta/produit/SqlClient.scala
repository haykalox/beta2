package com.beta.produit

import com.beta.RW._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.current_date

object SqlClient {
  def main(args: Array[String]): Unit = {


    val spark_ = new SparkConnector
    val spark = spark_.getSession()

    val df = spark.sql("select clients.id_client,nom,achats.qt from clients INNER JOIN (select id_client, SUM(qt) as Qt from achats group by id_client order by Qt desc limit 3 )achats ON achats.id_client=clients.id_client ")
      .withColumn("technical_partition", current_date())

    val dx = new Read
    val dw = dx.writeData(df,"/apps/hive/external/default/achat_Client")

    spark.sql("drop table if EXISTS achat_client")

    spark.sql(
      """CREATE EXTERNAL TABLE IF NOT EXISTS
        |achat_client (id_client Int,nom String,quantite_achat Int)
        |ROW FORMAT DELIMITED
        |FIELDS TERMINATED BY ';'
        |STORED AS TEXTFILE
        |PARTITIONED BY (technical_partition date)
        |LOCATION '/apps/hive/external/default/achat_Client/'
        |""".stripMargin)

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.listStatus(new Path("/apps/hive/external/default/achat_Client"))
      .filter(_.isDirectory)
      .map(_.getPath.getName.replaceFirst("technical_partition=",""))
      .foreach(fs =>
        spark.sql(s"""alter table achat_Client add if not exists partition(technical_partition='$fs')"""))
    spark.sql("SELECT * FROM achat_Client").show()


  }
}

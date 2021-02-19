package com.beta.produit

import com.beta.RW.{Read, SparkConnector}
import org.apache.spark.sql.functions.current_date

object SqlProduit {
  def main(args: Array[String]): Unit = {


    val spark_ = new SparkConnector
    val spark = spark_.getSession()

    val df = spark.sql("select produits.id_produit,nom,achats.qt from produits INNER JOIN (select id_produit,SUM(qt) as qt from achats group by id_produit order by qt asc limit 5)achats ON produits.id_produit=achats.id_produit ")
      .withColumn("technical_partition", current_date())

    val dx = new Read
    val dw = dx.writeData(df,"/apps/hive/external/default/achat_produit","achat_produit")

    spark.sql("SELECT * FROM achat_produit").show()


  }
}

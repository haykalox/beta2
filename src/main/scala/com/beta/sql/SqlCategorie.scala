package com.beta.sql

import com.beta.RW.{Read, SparkConnector}
import org.apache.spark.sql.functions.current_date

object SqlCategorie {
  def main(args: Array[String]): Unit = {


    val spark_ = new SparkConnector
    val spark = spark_.getSession()

    val df = spark.sql("select categories.id_categorie,categories.nom,produits.qt from categories INNER JOIN (select sum(achats.qt) as qt,produits.id_categorie from produits INNER JOIN achats ON produits.id_produit=achats.id_produit group by produits.id_categorie)produits ON categories.id_categorie=produits.id_categorie order by produits.qt desc limit 3")
      .withColumn("technical_partition", current_date())
    val dfc=df.count()

    val dx = new Read
    val dw = dx.writeData(df, "/apps/hive/external/default/categories_vente", "categories_vente",dfc)

    spark.sql("SELECT * FROM categories_vente").show()
    spark.sql("Show tblproperties categories_vente").show(false)
  }
}

package com.beta.spark

import com.beta.RW.{Read, SparkConnector}
import org.apache.spark.sql.functions.{col, current_date, sum}

object Produit {
  def main(args: Array[String]): Unit = {
    val Spark = new SparkConnector
    val spark = Spark.getSession()

    val dfr = new Read


    val dfp = dfr.readData("/data/sql/produits.csv")
    val dfa = dfr.readData("/data/sql/achats.csv")
    /*
    select produits.id_produit,nom,achats.qt from produits INNER JOIN (select id_produit,SUM(qt) as qt from achats group by id_produit order by qt asc limit 5)achats ON produits.id_produit=achats.id_produit
     */
    val df = dfp.join(dfa, dfp("id_produit")===dfa("id_produit")).drop(dfa("id_produit"))
      .withColumn("id_produit",col("id_produit").cast("Int"))
      .withColumn("qt",col("qt").cast("Int"))

    val dx = df.groupBy("id_produit","nom")
      .agg(sum("qt").as("quantite"))
      .orderBy(col("quantite").asc)
      .limit(5)
      .withColumn("technical_partition", current_date())
    val dfc=dx.count()
    dfr.writeData(dx,"/apps/hive/external/default/produit_spark","produit_spark",dfc)

    spark.sql("SELECT * FROM produit_spark").show()
    spark.sql("Show tblproperties produit_spark").show(false)




  }
}

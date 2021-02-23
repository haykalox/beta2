package com.beta.spark

import com.beta.RW.{Read, SparkConnector}
import org.apache.spark.sql.functions.{col, current_date, sum}

object Categorie {
  def main(args: Array[String]): Unit = {
    val Spark = new SparkConnector
    val spark = Spark.getSession()

    val dfr = new Read


    val dfc = dfr.readData("/data/sql/categories.csv")
    val dfa = dfr.readData("/data/sql/achats.csv")
    val dfp = dfr.readData("/data/sql/produits.csv")

    /*
    select categories.id_categorie,categories.nom,produits.qt from categories INNER JOIN (select sum(achats.qt) as qt,produits.id_categorie from produits INNER JOIN achats ON produits.id_produit=achats.id_produit group by produits.id_categorie)produits ON categories.id_categorie=produits.id_categorie order by produits.qt desc limit 3
     */

    val df1 = dfp.join(dfa, dfp("id_produit")===dfa("id_produit")).drop(dfa("id_produit"))
    val df = df1.join(dfc,dfc("id_categorie")===df1("id_categorie")).drop(dfc("id_categorie")).drop(df1("nom"))
    val df2=df.select("id_categorie","nom","qt")
      .withColumn("qt",col("qt").cast("Int"))

      val dfx=df2.groupBy("id_categorie","nom")
        .agg(sum("qt").as("quantite"))
        .orderBy(col("quantite").desc)
        .limit(3)
        .withColumn("technical_partition", current_date())


    dfr.writeData(dfx,"/apps/hive/external/default/categorie_spark","categorie_spark")

    spark.sql("SELECT * FROM categorie_spark").show()





  }
}

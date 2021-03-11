package com.beta.produit
import com.beta.RW._

object IngestionProduits {
  def main(args: Array[String]): Unit = {

    val Spark = new SparkConnector
    val spark = Spark.getSession()

    val dx = new Read
    val dr = dx.readData("/data/sql/produits.csv")
    val dfc=dr.count()
    val dw = dx.writeData(dr,"/apps/hive/external/default/produits/","produits",dfc)

    spark.sql("SELECT * FROM produits").show()

}}

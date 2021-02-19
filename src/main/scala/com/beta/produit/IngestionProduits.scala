package com.beta.produit
import com.beta.RW._

object IngestionProduits {
  def main(args: Array[String]): Unit = {

    val Spark = new SparkConnector
    val spark = Spark.getSession()

    val dx = new Read
    val dr = dx.readData("/data/sql/produits.csv")
    val dw = dx.writeData(dr,"/apps/hive/external/default/produits/","produits")

    spark.sql("SELECT * FROM produits").show()

}}

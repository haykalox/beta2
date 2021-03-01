package com.beta.produit

import com.beta.RW.{Read, SparkConnector}

object IngestionAchats {
  def main(args: Array[String]): Unit = {

    val Spark = new SparkConnector
    val spark = Spark.getSession()

    val dx = new Read
    val dr = dx.readData("/data/achats.csv")
    val dw = dx.writeData(dr,"/apps/hive/external/default/achats/","achats")

    spark.sql("SELECT * FROM achats").show()

  }
}

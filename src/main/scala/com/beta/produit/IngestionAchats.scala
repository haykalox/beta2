package com.beta.produit

import com.beta.RW.{Read, SparkConnector}

object IngestionAchats {
  def main(args: Array[String]): Unit = {

    val Spark = new SparkConnector
    val spark = Spark.getSession()

    val dx = new Read
    val dr = dx.readData("/data/achats.csv")
    val dfc=dr.count()

    val dw = dx.writeData(dr,"/apps/hive/external/default/achats/","achats",dfc)

    spark.sql("SELECT * FROM achats").show()

  }
}

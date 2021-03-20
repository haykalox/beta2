package com.beta.produit

import com.beta.RW.{Read, SparkConnector}

object IngestionClients {
  def main(args: Array[String]): Unit = {

    val Spark = new SparkConnector
    val spark = Spark.getSession()

    val dx = new Read
    val dr = dx.readData("/data/sql/clients.csv")
    val dfc=dr.count()
    val dw = dx.writeData(dr,"/apps/hive/external/default/clients/","clients",dfc)

    spark.sql("SELECT * FROM clients").show()
    spark.sql("Show tblproperties clients").show(false)

  }
}

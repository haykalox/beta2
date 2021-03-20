package com.beta.produit

import com.beta.RW.{Read, SparkConnector}

object IngestionCategories {
  def main(args: Array[String]): Unit = {

    val Spark = new SparkConnector
    val spark = Spark.getSession()

    val dx = new Read
    val dr = dx.readData("/data/sql/categories.csv")
    val dfc=dr.count()
    val dw = dx.writeData(dr,"/apps/hive/external/default/categories/","categories",dfc)

    spark.sql("SELECT * FROM categories").show()
    spark.sql("Show tblproperties categories").show(false)



  }
}

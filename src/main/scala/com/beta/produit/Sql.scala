package com.beta.produit

import com.beta.RW._

object Sql {
  def main(args: Array[String]): Unit = {


    val spark_ = new SparkConnector
    val spark = spark_.getSession()

    spark.sql("select * from achats group by id_clients desc").show()

  }
}

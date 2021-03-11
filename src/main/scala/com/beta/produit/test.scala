package com.beta.produit

import com.beta.RW.{Read, SparkConnector}

object test {
  def main(args: Array[String]): Unit = {

    val Spark = new SparkConnector
    val spark = Spark.getSession()

    val df = new Read

    val df1 = spark.read
      .text("/data/commandes.csv")

/*
val dt=df1.rdd.mapPartitionsWithIndex{
  case (index, iterator) => if(index==0) iterator.drop(1) else iterator
}



    import spark.implicits._
    val df2 = df1.map(f=>{
      val elements = f.getString(0).split("#####")
      (elements(0),elements(1),elements(2))
    })
val dx=df2.toDF("id_commande","dateheure","caisse")

  .show()


 */

  }
}

package com.beta.produit

import com.beta.RW.{Read, SparkConnector}

object test {
  def main(args: Array[String]): Unit = {

    val Spark = new SparkConnector
    val spark = Spark.getSession()

    val df = new Read

    val df1 = spark.read
      .option("header", "true")
      .option("mode", "dropmalformed")
      .csv("/data/commandes.csv")

    val schema = df1.schema.map(m => m.name)
    import spark.implicits._
    val df2 = df1.map(f=>{
      val elements = f.getString(0).split("#####")
      (elements(0),elements(1),elements(2))
    })
    df2.printSchema()
    df2.show()
val dx=df2.toDF(s"${schema}")
dx.show()

  }
}

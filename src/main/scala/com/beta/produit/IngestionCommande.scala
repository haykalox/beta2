package com.beta.produit

import com.beta.RW.SparkConnector
import org.apache.spark.sql.functions._

object IngestionCommande {
    def main(args: Array[String]): Unit = {
      val Spark = new SparkConnector
      val spark = Spark.getSession()

      val df1 = spark.read
        .format("csv")
        .option("header", "true")
        .option("mode", "dropmalformed")
        .option("inferSchema","true")
        .load("/data/commandes.csv")

      import spark.implicits._
      val df2 = df1.map(f=>{
        val elements = f.getString(0).split("#####")
        (elements(0),elements(1),elements(2))
      })
      df2.printSchema()
      df2.show()
      val dx=df2.toDF("id_commande","dateheure","caisse")
        .withColumn("technical_partition", current_date())
        .withColumn("responsable",lit(1))
        .withColumn("testd",to_timestamp(col("dateheure"),"dd/MM/yyyy HH:mm"))

     .select(
          col("id_commande"),
          date_format(col("testd"),"YYYY-MM-DD").as("jours"),
          date_format(col("testd"),"HH:MM:SS").as("heure"),
          col("caisse"),col("responsable"),
           col("technical_partition").cast("date")
              )
        .show()
/*
        df.writeData(dfx,"/apps/hive/external/default/commande","commande")

        spark.sql("SELECT * FROM commande").show()


 */
    }
  }

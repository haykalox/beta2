package com.beta.produit

import com.beta.RW.{Read, SparkConnector}
import org.apache.spark.sql.functions.{col, current_date, date_format, lit, to_timestamp}

object IngestionCommande {
    def main(args: Array[String]): Unit = {
      val Spark = new SparkConnector
      val spark = Spark.getSession()

      val df = new Read

      val df2 = spark.read
        .format("csv")
        .option("header", "true")
        .option("mode", "dropmalformed")
        .option("delimiter", "#")
        .option("inferSchema","true")
        .load("/data/commandes.csv")
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


        df.writeData(df2,"/apps/hive/external/default/commande","commande")

        spark.sql("SELECT * FROM commande").show()

    }
  }

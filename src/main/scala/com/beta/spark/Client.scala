package com.beta.spark

import com.beta.RW.{Read, SparkConnector}
import org.apache.spark.sql.functions.{col, current_date, sum}

object Client {
  def main(args: Array[String]): Unit = {
    val Spark = new SparkConnector
    val spark = Spark.getSession()

    val dfr = new Read


    val dfc = dfr.readData("/data/sql/clients.csv")
    val dfa = dfr.readData("/data/sql/achats.csv")
    /*
    select clients.id_client,nom,achats.qt from clients INNER JOIN (select id_client, SUM(qt) as Qt from achats group by id_client order by Qt desc limit 3 )achats ON achats.id_client=clients.id_client ")
     */
val df = dfc.join(dfa, dfc("id_client")===dfa("id_client")).drop(dfa("id_client"))
  .withColumn("id_client",col("id_client").cast("Int"))
  .withColumn("id_produit",col("id_produit").cast("Int"))
  .withColumn("qt",col("qt").cast("Int"))
    val dx = df.groupBy("id_client","nom")
      .agg(sum("qt").as("quantite"))
      .orderBy(col("quantite").desc)
      .limit(3)
      .withColumn("technical_partition", current_date())
    val dfcc=dx.count()

    dfr.writeData(dx,"/apps/hive/external/default/client_spark","client_spark",dfcc)

    spark.sql("SELECT * FROM client_spark").show()
    spark.sql("Show tblproperties client_spark").show(false)
  }
}

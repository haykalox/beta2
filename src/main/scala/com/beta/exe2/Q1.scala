package com.beta.exe2

import com.beta.RW.{Read, SparkConnector}
import org.apache.spark.sql.functions.{col, countDistinct, current_date}

object Q1 {
  def main(args: Array[String]): Unit = {

    val spark_ = new SparkConnector
    val spark = spark_.getSession()

    val dfs = spark.sql("select clients.id_client,clients.nom,achats.nombre_commande from clients INNER JOIN (select id_client, count(DISTINCT id_commande) as nombre_commande from achats group by id_client order by nombre_commande desc limit 1 ) achats ON achats.id_client=clients.id_client")
      .withColumn("technical_partition", current_date())
    val dfr = new Read

    val dfc = dfr.readData("/data/sql/clients.csv")
    val dfa = dfr.readData("/data/achats.csv")

    val df1 = dfc.join(dfa, dfc("id_client")===dfa("id_client")).drop(dfa("id_client"))
    val df2=df1.select("id_client","nom","id_commande")
      .withColumn("id_commande",col("id_commande").cast("Int"))

    val dfx=df2.groupBy("id_client","nom")
      .agg(countDistinct("id_commande").as("num_commande"))
      .orderBy(col("num_commande").desc)
      .limit(1)
      .withColumn("technical_partition", current_date())


    dfs.show()
    dfx.show()
  }

}

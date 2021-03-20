package com.beta.produit

import com.beta.RW.{Read, SparkConnector}
import org.apache.spark.sql.functions._

object IngestionCommande {
  def main(args: Array[String]): Unit = {
    val Spark = new SparkConnector
    val spark = Spark.getSession()

    val df = new Read

    val df1 = spark.read                                                                //lecture du csv en format txt
      .text("/data/commandes.csv")
    val dfc=df1.count()

    val dft=df1.rdd.mapPartitionsWithIndex{                                              //suppression du premiere ligne qui contienne les noms des colonnes
      case (index, iterator) => if(index==0) iterator.drop(1) else iterator
    }

    import spark.implicits._                                                            //creation des colonnes avec le separateur non commune
    val df2 = dft.map(f=>{                                                              // "#####"
      val elements = f.getString(0).split("#####")
      (elements(0),elements(1),elements(2))
    })

    val dfx=df2.toDF("id_commande","dateheure","caisse")                       //creation du dataframe avec les noms des colonnes
      .withColumn("technical_partition", current_date())                      //ajouter la colonne "technical_partition" avec le date du jour
      .withColumn("responsable",lit(1))                                //ajouter la colonne "responsable"avec un valeure commain pour tout les lignes
      .withColumn("testd",to_timestamp(col("dateheure"),"dd/MM/yyyy HH:mm"))  //convertire la colonne "dateheure" en formate date et heure

      .select(                                                                           //la selecction des colonne
        col("id_commande"),                                                 // selection la colone "id_commande"
        date_format(col("testd"),"YYYY-MM-DD").as("jours"),   //selection la partie date du colonne "dateheure"
        date_format(col("testd"),"HH:MM:SS").as("heure"),     //selection la partie heure du colonne "dateheure"
        col("caisse"),col("responsable"),                         //selection du colonne "responsable"
        col("technical_partition").cast("date")                       //selection du colonne "technical _partition" avec la fromat date .cast
      )

    df.writeData(dfx,"/apps/hive/external/default/commande","commande",dfc)



    spark.sql("SELECT * FROM commande").show()
    spark.sql("Show tblproperties commande").show(false)



  }
}
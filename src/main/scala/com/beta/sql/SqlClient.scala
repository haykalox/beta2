package com.beta.sql

import com.beta.RW.{Read, SparkConnector}
import org.apache.spark.sql.functions.current_date

object SqlClient {
  def main(args: Array[String]): Unit = {


    val spark_ = new SparkConnector
    val spark = spark_.getSession()

    val df = spark.sql("select clients.id_client,nom,achats.qt from clients INNER JOIN (select id_client, SUM(qt) as Qt from achats group by id_client order by Qt desc limit 3 )achats ON achats.id_client=clients.id_client ")
      .withColumn("technical_partition", current_date())
    val dfc=df.count()

    val dx = new Read
    val dw = dx.writeData(df, "/apps/hive/external/default/achat_Client", "achat_Client",dfc)

    spark.sql("SELECT * FROM achat_Client").show()
    spark.sql("Show tblproperties achat_Client").show(false)

  }
}

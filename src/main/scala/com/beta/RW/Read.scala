package com.beta.RW


import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.current_date



class Read {
  val spark_ = new SparkConnector
  val spark = spark_.getSession()

  def readData(location: String): DataFrame = {

    spark.read
      .format("csv")
      .option("header", "true")
      .option("mode", "dropmalformed")
      .option("delimiter", ";")
      .load(location)
      .withColumn("technical_partition", current_date())
  }

    def writeData(dr: DataFrame ,locationD: String,tb: String,count: Long): Unit = {
      case class tbPro(tableName :String,published:String,schema:String,size:Long)

      dr.write
        .format("csv")
        .mode("overwrite")
        .option("delimiter", ";")
        .partitionBy("technical_partition")
        .save(locationD)

      val data_schema = dr.schema.map(m => m.name+" "+m.dataType.typeName).mkString(", ")

      val schema = data_schema.replace(", technical_partition date","")

      spark.sql(s"""drop table if EXISTS $tb""")

      spark.sql(
        s"""CREATE EXTERNAL TABLE IF NOT EXISTS
          |$tb ($schema)
          |ROW FORMAT DELIMITED
          |FIELDS TERMINATED BY ';'
          |STORED AS TEXTFILE
          |PARTITIONED BY (technical_partition date)
          |LOCATION '/apps/hive/external/default/$tb'
          |""".stripMargin)


      val fx = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      fx.listStatus(new Path(s"""/apps/hive/external/default/$tb"""))
        .filter(_.isDirectory)
        .map(_.getPath.getName.replaceFirst("technical_partition=",""))
        .foreach(fx =>
          spark.sql(s"""alter table $tb add if not exists partition(technical_partition='$fx')"""))

      val dc=tbPro(tb,"True",data_schema,count)
      val config = ConfigFactory.load("application.conf").getConfig("tables")

      if ( dc.tableName == "commande" ) {
        val tbl = config.getConfig("commande")
        val published = tbl.getString("published")
        val Commentaire = tbl.getString("Commentaire")
        spark.sql(s"ALTER TABLE ${dc.tableName} SET TBLPROPERTIES ( 'published'='$published')")
        spark.sql(s"ALTER TABLE ${dc.tableName} SET TBLPROPERTIES ('Commentaire'='$Commentaire')")
      }
      else if ( dc.tableName == "categories" ) {
        val tbl = config.getConfig("categories")
        val published = tbl.getString("published")
        val Commentaire = tbl.getString("Commentaire")
        spark.sql(s"ALTER TABLE ${dc.tableName} SET TBLPROPERTIES ( 'published'='$published')")
        spark.sql(s"ALTER TABLE ${dc.tableName} SET TBLPROPERTIES ('Commentaire'='$Commentaire')")
      }
      else if ( dc.tableName == "produits" ) {
        val tbl = config.getConfig("produits")
        val published = tbl.getString("published")
        val Commentaire = tbl.getString("Commentaire")
        spark.sql(s"ALTER TABLE ${dc.tableName} SET TBLPROPERTIES ( 'published'='$published')")
        spark.sql(s"ALTER TABLE ${dc.tableName} SET TBLPROPERTIES ('Commentaire'='$Commentaire')")
      }
      else if ( dc.tableName == "clients" ) {
        val tbl = config.getConfig("clients")
        val published = tbl.getString("published")
        val Commentaire = tbl.getString("Commentaire")
        spark.sql(s"ALTER TABLE ${dc.tableName} SET TBLPROPERTIES ( 'published'='$published')")
        spark.sql(s"ALTER TABLE ${dc.tableName} SET TBLPROPERTIES ('Commentaire'='$Commentaire')")
      }
      else{
      spark.sql(s"ALTER TABLE ${dc.tableName} SET TBLPROPERTIES ( 'published'='${dc.published}')")

      spark.sql(s"ALTER TABLE ${dc.tableName} SET TBLPROPERTIES ( 'schema'='${dc.schema}')")
      }
      if( dc.size <5)
      spark.sql(s"ALTER TABLE ${dc.tableName} SET TBLPROPERTIES ( 'size'='small')")
      else
      spark.sql(s"ALTER TABLE ${dc.tableName} SET TBLPROPERTIES ( 'size'='big')")



    }
  }
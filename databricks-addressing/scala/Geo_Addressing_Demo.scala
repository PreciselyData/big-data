// Databricks notebook source
// MAGIC %md
// MAGIC # Geo Addressing Demo
// MAGIC In this notebook the geocoder will be configured, some sample data will be created, and geocodes will be executed using the data.
// MAGIC Before executing this notebook make sure to have completed executing all cells in Addressing Installation notebook.

// COMMAND ----------

// DBTITLE 1,Java Version
// MAGIC %sh
// MAGIC # make sure java version is 11
// MAGIC # from databricks runtime 15.*,  you can add JNAME=zulu17-ca-amd64 as environment variable and restart the cluster
// MAGIC # till databricks runtime version 15.*. you can add JNAME=zulu11-ca-amd64 as environment variable and restart the cluster
// MAGIC java -version

// COMMAND ----------

// DBTITLE 1,Spark Session
import org.apache.spark.sql._
val spark: SparkSession =
  SparkSession
    .builder()
    .config("spark.sql.legacy.allowUntypedScalaUDF", value = true)
    .getOrCreate()

// COMMAND ----------

// DBTITLE 1,Global Variables


// COMMAND ----------

// DBTITLE 1,Create Geocode UDF
import com.precisely.bigdata.addressing.spark.api.AddressingBuilder
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import java.io.File

val addressingSdkLocation = "/dbfs/precisely/addressing/geo-addressing-bigdata-distribution-5.2.1"
val resourceLocation = s"$addressingSdkLocation/resources"
val outputFields = Seq("customFields['PB_KEY']", "address.formattedStreetAddress", "address.formattedLocationAddress", "location.feature.geometry.coordinates.x", "location.feature.geometry.coordinates.y")
val geocodeUdf: UserDefinedFunction = new AddressingBuilder()
      .withResourcesLocation(resourceLocation)
      .udfBuilder()
      .withOutputFields(outputFields: _*)
      .withErrorField("error")
      .withResultAsJSON("resultJSON")
      .forGeocode()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Build some test data or create your own udf.

// COMMAND ----------

var df = Seq(("350 jordan rd ny 12180", "USA"), 
  ("1 Global View troy ny 12180", "USA"), 
  ("222 Jersey City Blvd Jersey City NJ 7305", "USA")
).toDF("ADDRESS", "COUNTRY")

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC ## NOTE: Please run some warm up calls to start the Geo Addressing Engine, before running on large data.
// MAGIC Also, as the geocode operation can be computationally expensive, Adding a call to "df.persist()" directly after the geocode function should ensure that each record calls the geocode function only once.

// COMMAND ----------

// DBTITLE 1,Run the Geocode Query
  val geocodeDF = df.withColumn("result", geocodeUdf(map(
          lit("addressLines[0]"), col("ADDRESS"),
          lit("country"), col("COUNTRY")
    )))
      .persist()
      .select("*", "result.*").drop("result")

display(geocodeDF)

// COMMAND ----------

// MAGIC %md
// MAGIC # Registering the geo addressing functionalities as UDFs and running the SQL queries
// MAGIC
// MAGIC You can now register the geo addressing functionalities as UDFs and start running SQL queries.

// COMMAND ----------

val geocodeUdf: UserDefinedFunction = new AddressingBuilder()
      .withResourcesLocation(resourceLocation)
      .udfBuilder()
      .withOutputFields(outputFields: _*)
      .withErrorField("error")
      .withResultAsJSON("resultJSON")
      .forGeocode("PreciselyGeocode")

df.createOrReplaceTempView("inputTable")

val geocodeSqlDf = spark.sql("select *, PreciselyGeocode(map('addressLines[0]', ADDRESS, 'country', COUNTRY)) as result from inputTable")
        .persist().select("*", "result.*").drop("result")

display(geocodeSqlDf)

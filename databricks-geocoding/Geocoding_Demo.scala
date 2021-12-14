// Databricks notebook source
// MAGIC %md
// MAGIC # Addressing Geocoding Demo
// MAGIC In this notebook the geocoder will be configured, some sample data will be created, and geocodes will be executed using the data.
// MAGIC Before executing this notebook make sure to have completed executing all cells in Geocoding Installation notebook.

// COMMAND ----------

// DBTITLE 1,Global Variables
// GeocodingRootDBFS will be the same path specified in the installation script
val GeocodingRootDBFS = "/geocoding"

// where to write the generated preferences file
val PreferencesFileDBFS = s"$GeocodingRootDBFS/addressing.yaml"

// These should not need to be modified
val SDKLocationLocal = s"/dbfs$GeocodingRootDBFS/sdk"
val PreferencesFileLocal = s"/dbfs$PreferencesFileDBFS"
val DataLocationLocal = s"/dbfs$GeocodingRootDBFS/data"
val ExtractLocationLocal = "/precisely/data"

// COMMAND ----------

// MAGIC %md
// MAGIC ## Geocode Preferences

// COMMAND ----------

// DBTITLE 1,Update Geocoder Preferences
dbutils.fs.put(PreferencesFileDBFS,"""
---
config:
  default:
    preferences:
      returnAllInfo: true
      clientCoordSysName: "epsg:4326"
""", true)
dbutils.fs.head(PreferencesFileDBFS, 1024*1024*10)

// COMMAND ----------

// DBTITLE 1,Create Geocode UDF
import com.precisely.bigdata.addressing.spark.api.AddressingBuilder
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import java.io.File

def getListOfSubDirectories(dir: File): List[String] = dir.listFiles.filter(_.isDirectory).map(_.getName).toList
val subFolders = getListOfSubDirectories(new File(s"/dbfs$GeocodingRootDBFS/sdk"))
val ResourcesLocationLocal = s"/dbfs$GeocodingRootDBFS/sdk/" + subFolders.filter(_.startsWith("spectrum-bigdata-addressing"))(0) + "/resources/"

val geocodeUdf: UserDefinedFunction = new AddressingBuilder()
      .withResourcesLocation(ResourcesLocationLocal)
      .withDataLocations(DataLocationLocal)
      .withExtractionLocation(ExtractLocationLocal)
      .udfBuilder()
      .withPreferencesFile(PreferencesFileLocal)
      .withOutputFields("customFields['PB_KEY']", "address.formattedStreetAddress", "address.formattedLocationAddress", "location.feature.geometry.coordinates.x", "location.feature.geometry.coordinates.y")
      .withErrorField("error")
      .forGeocode()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Build some test data

// COMMAND ----------

var df = Seq(
  ("350 jordan rd","troy","ny","12180"),
  ("1 Global View","troy","ny","12180"),
  ("222 Jersey City Blvd", "Jersey City", "NJ", "7305")
).toDF("address", "city", "state", "postcode")

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Execute the Geocode
// MAGIC Note: Due to the behavior of the spark query execution planner, the geocode function could be executed multiple times for every record.  Because the geocode operation can be computationally expensive, this should be avoided.  Adding a call to "df.persist()" directly after the geocode function should ensure that each record calls the geocode function only once.

// COMMAND ----------

// DBTITLE 1,Run the Geocode Query
  df = df.withColumn("result", geocodeUdf(map(
          lit("street"), $"address",
          lit("city"), $"city",
          lit("admin1"), $"state",
          lit("postalCode"), $"postcode",
          lit("country"), lit("USA")
    )))
      .persist()
      .select("*", "result.*").drop("result")

display(df)

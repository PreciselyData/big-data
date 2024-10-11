// Databricks notebook source
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder()
      .master("yarn")
      .appName("pointInPolygon-li-sdk")
      .config("spark.sql.legacy.allowUntypedScalaUDF", true)
      .getOrCreate();

// COMMAND ----------

import com.pb.downloadmanager.api.downloaders.LocalFilePassthroughDownloader
import com.pb.downloadmanager.api.downloaders.hadoop.{HDFSDownloader, S3Downloader}
import com.pb.downloadmanager.api.{DownloadManagerBuilder, PosixPermissionsManagerBuilder}
import com.precisely.bigdata.li.spark.api.SpatialAPI
import com.precisely.bigdata.li.spark.app.DriverConstants.AllowUntypedScalaUDF
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

// COMMAND ----------

val fabricPath = "" // The HDFS path to input file containing location data
val tableFilePath = "" // The HDFS path to spatial table
val outputPath = "" // Output path
val tabFileType = "TAB" // Type of spatial data provided
val tableFileName = "" // Spatial table file name
val longitude= "" // Longitude column name
val latitude = "" // Latitude column name
val includeEmptySearchResults = true // if true then an empty search will keep the original input row and the new columns will be null and if false then an empty search will result in the row not appearing in the outputted DataFrame
val outputFields = Seq("ZIP", "Name") // Fields from the polygon table to include in the output
val fabricDF = spark.read.option("header","true").option("delimiter", ",").csv(fabricPath)
fabricDF.show(5)

// COMMAND ----------

// Perform Point In polygon operation
SpatialAPI.pointInPolygon(inputDF = fabricDF, tableFileType = tabFileType, tableFilePath = tableFilePath, tableFileName = tableFileName, longitude = longitude, latitude = latitude, includeEmptySearchResults = includeEmptySearchResults, 
  outputFields = outputFields)
  .write.mode(SaveMode.Overwrite).option("header", true).format("csv")
  .save(outputPath)

// COMMAND ----------

// Read output
val output = spark.read.option("header","true").csv(outputPath)
output.show(50)

// COMMAND ----------

   

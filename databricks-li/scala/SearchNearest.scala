// Databricks notebook source
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder()
      .master("yarn")
      .appName("searchNearest-li-sdk")
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

val fabricPath = "" // The HDFS path to input file
val tableFilePath = "" // The HDFS path to spatial table
val outputPath = "" // Output path
val tableFileType = "TAB" // Type of spatial data provided
val tableFileName = "" // Spatial table file name
val geometryStringType = "GeoJSON" // Format of geometry string data provided
val distanceValue =  // The absolute value of buffer length around point 1 to search for point 2
val distanceUnit = "" // unit of measurement for distanceUnit parameter, e.g. mi
val geometryColumnName = "geometry" // Geometry column name for input data
val includeEmptySearchResults = true // if true then an empty search will keep the original input row and the new columns will be null and if false then an empty search will result in the row not appearing in the outputted DataFrame
val outputFields = Seq() // Fields from the polygon table to include in the output
val maxCandidates =  // Maximum number of candidates
val distanceColumnName = "" // Distance column name in output
val fabricDF = spark.read.option("header","true").option("delimiter", ",").csv(fabricPath)
fabricDF.show(5)

// COMMAND ----------

// Perform Search Nearest Operation
SpatialAPI.searchNearest(inputDF = fabricDF, tableFileType = tableFileType, tableFilePath = tableFilePath, tableFileName = tableFileName, libraries = null, maxCandidates = maxCandidates, geometryColumnName = geometryColumnName, distanceValue = distanceValue, distanceUnit = distanceUnit, distanceColumnName = distanceColumnName, geometryStringType = geometryStringType, includeEmptySearchResults = includeEmptySearchResults, outputFields = outputFields)
  .write.mode(SaveMode.Overwrite).option("header", true).format("csv")
  .save(outputPath)

// COMMAND ----------

// Read Output
val output = spark.read.option("header","true").option("delimiter",",").csv(outputPath)
output.show(5)

// COMMAND ----------



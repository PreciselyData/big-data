// Databricks notebook source
import org.apache.spark.sql.SparkSession
  val spark = SparkSession.builder()
      .master("yarn")
      .appName("generateHexagon-li-sdk")
      .config("spark.sql.legacy.allowUntypedScalaUDF", true)
      .getOrCreate();
import com.precisely.bigdata.li.spark.api.SpatialAPI
import com.precisely.bigdata.li.spark.app.DriverConstants.AllowUntypedScalaUDF
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

// COMMAND ----------

val minLongitude =  // The bottom left longitude of the bounding box
val minLatitude =  // The bottom left latitude of the bounding box
val maxLongitude =  // The upper right longitude of the bounding box
val maxLatitude =  // The upper right latitude of the bounding box
val hexLevel =  // The level to generate hexagons for. Must be between 1 and 11.
val containerLevel =  // A hint for providing some parallel hexagon generation. Must be less than the hexLevel property.
val numOfPartitions =   // Number of partitions
val maxNumberOfRows =  // Max number of rows per partition. This number will depend on available memory for executor.
val outputPath = "" // Output path

// COMMAND ----------

// Peform Hexgen operation
val df: DataFrame = SpatialAPI.generateHexagon(sparkSession = spark, minLongitude = minLongitude, minLatitude = minLatitude,
  maxLongitude = maxLongitude, maxLatitude = maxLatitude,
  hexLevel = hexLevel, containerLevel = containerLevel,
  numOfPartitions = numOfPartitions, maximumNumOfRowsPerPartition = maxNumberOfRows)
df.write.mode(SaveMode.Overwrite).option("header", true).format("csv").save(outputPath)

// COMMAND ----------

// Read output
val output = spark.read.option("header","true").csv(outputPath)
output.show(50)

// COMMAND ----------



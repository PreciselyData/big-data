// Databricks notebook source
import org.apache.spark.sql.SparkSession
  val spark = SparkSession.builder()
      .master("yarn")
      .appName("joinByDistance-li-sdk")
      .config("spark.sql.legacy.allowUntypedScalaUDF", true)
      .getOrCreate();

// COMMAND ----------

import com.precisely.bigdata.li.spark.api.SpatialAPI
import com.precisely.bigdata.li.spark.api.util.DistanceJoinOption
import com.precisely.bigdata.li.spark.api.util.DistanceJoinOption.DistanceJoinOption
import com.precisely.bigdata.li.spark.api.util.LimitMethods
import com.precisely.bigdata.li.spark.api.util.LimitMethods.LimitMethods
import com.precisely.bigdata.li.spark.app.DriverConstants.AllowUntypedScalaUDF
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.rogach.scallop.ScallopOption
import org.rogach.scallop._


// COMMAND ----------

val poiCSV = "" //The path to the first input file containing point data
val addressFabricCSV = "" // //The path to the second input file containing point data
val searchRadius =  // The absolute value of buffer length around point 1 to search for point 2
val distanceUnit = "mi" // unit of measurement for distanceUnit parameter, e.g. mi
val longitude1 = "" // The longitude column name of the first dataframe
val latitude1 = "" // The latitude column name of the first dataframe
val longitude2 = "" // The longitude column name of the second dataframe
val latitude2 = "" // The latitude column name of the second dataframe
val distanceColumnName = "outputDistance" // The output distance column name
val geoHashPrecision =  // The geohash precision
val limitMethod = LimitMethods.RowNumber // The limit method name
val output = "" // Output path
val limitMatches = 7 // Limit Value as the DistanceJoinOption

// COMMAND ----------

def getJoinOptions(limit: Int, distanceColumnName:String, limitMethod: LimitMethods): Map[DistanceJoinOption, Any] = {
    var options = Map.empty[DistanceJoinOption, Any]
    if (distanceColumnName != null)
      options += (DistanceJoinOption.DistanceColumnName -> distanceColumnName)
    if (limit > 0)
      options += (DistanceJoinOption.LimitMatches -> limit)
    if (limitMethod != null)
      options += (DistanceJoinOption.LimitMethod -> limitMethod)
    options
}

// COMMAND ----------

var dfUSAPoi = spark.read.format("csv").option("header", "true").option("delimiter", ",").option("inferSchema", "true").load(poiCSV)
// dfUSAPoi.show(2)

var dfUSAddressFabric = spark.read.format("csv").option("header", "true").option("delimiter", ",").option("inferSchema", "true").load(addressFabricCSV)
// dfUSAddressFabric.show(2)

// COMMAND ----------

// Perform Join by distance 
val df: DataFrame = SpatialAPI.joinByDistance(df1 = dfUSAPoi, df2 = dfUSAddressFabric,
  df1Longitude = longitude1, df1Latitude = latitude1, df2Longitude = longitude2, df2Latitude = latitude2, searchRadius = searchRadius, distanceUnit = distanceUnit,geoHashPrecision = geoHashPrecision, options = getJoinOptions(limitMatches, distanceColumnName, limitMethod))

// COMMAND ----------

df.write.mode(SaveMode.Overwrite).option("header", true).format("csv").save(output)

// COMMAND ----------

val result = spark.read.option("header","true").csv(output)
result.show(5)

// COMMAND ----------



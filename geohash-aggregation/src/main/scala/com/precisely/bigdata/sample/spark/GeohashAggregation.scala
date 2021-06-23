/*
 * Copyright 2019,2020 Precisely
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.precisely.bigdata.sample.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction
import com.mapinfo.midev.persistence.wkt.WKTUtilities
import com.pb.lisdk.grid.geohash.GeoHash

object GeohashAggregation {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setIfMissing("spark.master", "local[*]")

    val versionInfo = org.apache.spark.SPARK_VERSION.split('=')(0).split('.')
    if(versionInfo(0).toInt == 3 && versionInfo(1).toInt == 0) {
      sparkConf.setIfMissing("spark.sql.legacy.allowUntypedScalaUDF", "true")
    }

    val session = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    val inputPath = args(0)
    val outputPath = args(1)

    // Read the input data into a DataFrame.
    // Result Columns: (Agency, Longitude, Latitude)
    val inputDf = session.read.schema(schema).option("sep", "\t").csv(inputPath)

    // Add Geohash column.
    // Result Columns: (Agency, Longitude, Latitude, Geohash)
    val dfWithGeohash = inputDf.withColumn("geohash", geohash(col("LONGITUDE"), col("LATITUDE"), lit(7)))

    // Compute the total record count for each Geohash.
    // Result Columns: (Geohash, Count)
    val geohashCounts = dfWithGeohash.groupBy(col("geohash")).count()

    // Compute the agency count for each Geohash.
    // Result Columns: (Geohash, DSNY, DEP, NYPD, HPD, DOT)
    val dfWithAgencyCounts = dfWithGeohash.groupBy(col("geohash")).pivot("agency").count()

    // Join back with the geohashTotals dataframe to get the total for all agencies in a Geohash.
    // Result Columns: (Geohash, DSNY, DEP, NYPD, HPD, DOT, Count)
    val dfWithCounts = dfWithAgencyCounts.join(geohashCounts, "geohash")

    // Add a Well Known Text representation of the geohash boundary. This could be used in visualization or further calculations.
    // Result Columns: (Geohash, DSNY, DEP, NYPD, HPD, DOT, Count, Polygon)
    val dfWithWKT = dfWithCounts.withColumn("polygon", toWKT(col("geohash")))

    dfWithWKT.write.mode(SaveMode.Overwrite).parquet(outputPath)
  }

  val schema = StructType(Array(
    StructField("agency", StringType, nullable = false),
    StructField("LONGITUDE", DoubleType, nullable = false),
    StructField("LATITUDE", DoubleType, nullable = false))
  )

  def geohash: UserDefinedFunction = udf((longitude: Double, latitude: Double, precision: Int) => {
    GeoHash.encode(longitude, latitude, precision).getHash
  })

  def toWKT: UserDefinedFunction = udf((geohash: String) => {
    WKTUtilities.toWKT(GeoHash.decode(geohash).asFeatureGeometry)
  })
}
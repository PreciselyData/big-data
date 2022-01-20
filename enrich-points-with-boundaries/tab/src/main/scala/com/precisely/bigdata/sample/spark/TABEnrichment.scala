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

import com.mapinfo.midev.coordsys.CoordSysConstants
import com.mapinfo.midev.geometry.impl.Point
import com.mapinfo.midev.geometry.{DirectPosition, SpatialInfo}
import com.mapinfo.midev.language.filter.{FilterSearch, GeometryFilter, GeometryOperator, SelectList}
import com.mapinfo.midev.persistence.wkt.WKTUtilities
import com.pb.bigdata.li.spark.api.SpatialImplicits._
import com.pb.bigdata.li.spark.api.table.TableBuilder
import com.pb.downloadmanager.api.DownloadManagerBuilder
import com.pb.downloadmanager.api.downloaders.LocalFilePassthroughDownloader
import com.pb.downloadmanager.api.downloaders.hadoop.{HDFSDownloader, S3Downloader}
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

object TABEnrichment {

  def main(args: Array[String]): Unit = {
    val tabDirectory = args(0)
    val addressFabricPath = args(1)
    val outputPath = args(2)
    val downloadLocation = args(3)

    val sparkConf = new SparkConf().setIfMissing("spark.master", "local[*]")

    val versionInfo = org.apache.spark.SPARK_VERSION.split('=')(0).split('.')
    if(versionInfo(0).toInt == 3 && versionInfo(1).toInt == 0) {
      sparkConf.setIfMissing("spark.sql.legacy.allowUntypedScalaUDF", "true")
    }


    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val downloadManager = new DownloadManagerBuilder(downloadLocation)
      .addDownloader(new HDFSDownloader(spark.sparkContext.hadoopConfiguration))
      .addDownloader(new S3Downloader(spark.sparkContext.hadoopConfiguration))
      .addDownloader(new LocalFilePassthroughDownloader())
      .build()

    val table = TableBuilder.NativeTable(tabDirectory, "CrimeIndex_2019_Sample.TAB").withDownloadManager(downloadManager).build()

    val contains = (x : Double, y: Double) => {
      val selectList = new SelectList (
        "composite_crime_idx",
        "violent_crime_idx",
        "aggravated_assault_idx",
        "property_crime_idx",
        "arson_idx",
        "burglary_idx",
        "auto_theft_idx",
        "composite_crime_category",
        "violent_crime_category",
        "aggravated_assault_category",
        "property_crime_category",
        "arson_category",
        "burglary_category",
        "auto_theft_category"
      )
      val point = new Point(SpatialInfo.create(CoordSysConstants.longLatWGS84), new DirectPosition(x, y))
      // create the point-in-polygon filter
      val filter = new GeometryFilter("OBJ", GeometryOperator.CONTAINS, point)
      // create a search for all the specified columns and the specified filter
      val filterSearch = new FilterSearch(selectList, filter, null)
      table.search(filterSearch)
    }

    //HAD-6529 assign output of withSpatialSearchColumns() in a DataFrame and  use .withColumns() method to create WKT and other formats to store in dataframe and then save on disk.
    val df = spark.read.option("delimiter", "\t").option("header", "true").csv(addressFabricPath)
      .select("PBKEY", "LON", "LAT")
      .withSpatialSearchColumns(Seq(col("LON").cast(DoubleType), col("LAT").cast(DoubleType)), contains, includeEmptySearchResults = false)

    /*
     LI API supports attributes converters to convert a geometry to multiple formats.
     Here, adding a new column named "wkt" to store coordinates in WKT format.
     Similarly, geometries can be converted to WKB format using WKBUtilities as well.
     */
    val dfwithWKT = df.withColumn("wkt", toWKT(col("LON"), col("LAT")))
    dfwithWKT.write.mode(SaveMode.Overwrite).parquet(outputPath)
  }

  def toWKT: UserDefinedFunction = udf((x : Double, y: Double) => {
    WKTUtilities.toWKT(new Point(SpatialInfo.create(CoordSysConstants.longLatWGS84), new DirectPosition(x, y)))
  })
}

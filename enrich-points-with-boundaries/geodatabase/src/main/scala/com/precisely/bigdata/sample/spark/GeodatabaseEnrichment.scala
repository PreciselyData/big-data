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
import com.pb.bigdata.li.spark.api.SpatialImplicits._
import com.pb.bigdata.li.spark.api.table.TableBuilder
import com.pb.downloadmanager.api.DownloadManagerBuilder
import com.pb.downloadmanager.api.downloaders.LocalFilePassthroughDownloader
import com.pb.downloadmanager.api.downloaders.hadoop.{HDFSDownloader, S3Downloader}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

object GeodatabaseEnrichment {

  def main(args: Array[String]): Unit = {
    val geodatabaseDirectory = args(0)
    val addressFabricPath = args(1)
    val outputPath = args(2)
    val downloadLocation = args(3)

    val sparkConf = new SparkConf().setIfMissing("spark.master", "local[*]")
    var sparkMajorVersion=org.apache.spark.SPARK_VERSION.split('=')(0).split('.')(0).toInt
    if(sparkMajorVersion >= 3){
      var sparkMinorVersion=org.apache.spark.SPARK_VERSION.split('=')(0).split('.')(1).toInt
      if(sparkMajorVersion == 3 && sparkMinorVersion == 0){
        sparkConf.setIfMissing("spark.sql.legacy.allowUntypedScalaUDF","true")
      }

    }
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val downloadManager = new DownloadManagerBuilder(downloadLocation)
      .addDownloader(new HDFSDownloader(spark.sparkContext.hadoopConfiguration))
      .addDownloader(new S3Downloader(spark.sparkContext.hadoopConfiguration))
      .addDownloader(new LocalFilePassthroughDownloader())
      .build()

    val table = TableBuilder.GeodatabaseTable(geodatabaseDirectory, "CrimeIndex_2019_Sample").withDownloadManager(downloadManager).build()

    val contains = (x : Double, y: Double) => {
      val selectList = new SelectList (
        "c_crime_i", // composite crime index
        "v_crime_i", // violent crime index
        "assault_i", // aggrevated assault index
        "p_crime_i", // property crime index
        "arson_i", // arson index
        "burglary_i", // burglary index
        "a_theft_i", // auto theft index
        "c_crime_c", // composite crime category
        "v_crime_c", // violent crime category
        "assault_c", // aggrevated assault category
        "p_crime_c", // property crime category
        "arson_c", // arson category
        "burglary_c", // burglary category
        "a_theft_c" // auto theft category
      )
      val point = new Point(SpatialInfo.create(CoordSysConstants.longLatWGS84), new DirectPosition(x, y))
      // create the point-in-polygon filter
      val filter = new GeometryFilter("SHAPE", GeometryOperator.CONTAINS, point)
      // create a search for all the specified columns and the specified filter
      val filterSearch = new FilterSearch(selectList, filter, null)
      table.search(filterSearch)
    }

    spark.read.option("delimiter", "\t").option("header", "true").csv(addressFabricPath)
      .select("PBKEY", "LON", "LAT")
      .withSpatialSearchColumns(Seq(col("LON").cast(DoubleType), col("LAT").cast(DoubleType)), contains, includeEmptySearchResults = false)
      .write.mode(SaveMode.Overwrite).parquet(outputPath)
  }
}

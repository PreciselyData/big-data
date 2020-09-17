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
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

import com.mapinfo.midev.unit.{Length, LinearUnit}
import com.pb.bigdata.li.spark.api.SpatialImplicits._

object PointEnrichment {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setIfMissing("spark.master", "local[*]")
    sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    val session = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    val poiPath = args(0)
    val addressFabricPath = args(1)
    val outputPath = args(2)

    // load the poi into a DataFrame, only choosing columns needed to reduce write time to disk
    val poiDf = PoiInput.open(session, poiPath).select("NAME", "PB_ID", "LONGITUDE", "LATITUDE")

    // load the data fabric into a DataFrame, only choosing columns needed to reduce write time to disk
    val addressFabricDf = AddressFabricInput.open(session, addressFabricPath).select("PBKEY", "LON", "LAT")

    // Use Length object to define distance variable use when joining the two
    // point datasets by some max distance
    val distance = new Length(500, LinearUnit.FOOT)

    /*
    Here we will call the dataframe implicit class method to perform the join.  Parameters are:
    - dataframe of points to join to
    - Longitude of dataframe one
    - Latitude of dataframe one
    - Longitude of dataframe two
    - Latitude of dataframe two
    - the max search distance for the join
    - the Geohash level
     */
    val df = poiDf.joinByDistance(addressFabricDf, poiDf("LONGITUDE"), poiDf("LATITUDE"), addressFabricDf("LON"), addressFabricDf("LAT"), distance, 7)

    // writing out to parquet file as it results in much smaller disk space usage
    df.write.mode(SaveMode.Overwrite).parquet(outputPath)
  }
}

object AddressFabricInput {
  // set these parameters according to the CSV
  def open(session: SparkSession, csvPath: String): DataFrame = {
    session.read
      .option("delimiter", "\t")
      .option("header", "true")
      .schema(addressFabricSchema)
      .csv(csvPath)
  }

  private def addressFabricSchema = {
    // this is the schema for the address fabric data
    StructType(Array(
      StructField("PBKEY", StringType),
      StructField("ADD_NUMBER", StringType),
      StructField("STREETNAME", StringType),
      StructField("UNIT_DES", StringType),
      StructField("UNIT_NUM", StringType),
      StructField("CITY", StringType),
      StructField("STATE", StringType),
      StructField("ZIPCODE", StringType),
      StructField("TYPE", StringType),
      StructField("FIPS", StringType),
      StructField("LAT", DoubleType),
      StructField("LON", DoubleType),
      StructField("PARENT", StringType),
      StructField("PROP_TYPE", StringType)
    ))
  }
}

object PoiInput {
  // set these parameters according to the CSV
  def open(session: SparkSession, csvPath: String): DataFrame = {
    session.read
      .option("delimiter", "|")
      .option("header", "true")
      .schema(poiSchema)
      .csv(csvPath)
  }

  private def poiSchema = {
    StructType(Array(
      StructField("NAME", StringType),
      StructField("BRANDNAME", StringType),
      StructField("PB_ID", StringType),
      StructField("TRADE_NAME", StringType),
      StructField("FRANCHISE_NAME", StringType),
      StructField("ISO3", StringType),
      StructField("AREANAME4", StringType),
      StructField("AREANAME3", StringType),
      StructField("AREANAME2", StringType),
      StructField("AREANAME1", StringType),
      StructField("STABB", StringType),
      StructField("POSTCODE", StringType),
      StructField("FORMATTEDADDRESS", StringType),
      StructField("MAINADDRESSLINE", StringType),
      StructField("ADDRESSLASTLINE", StringType),
      StructField("LONGITUDE", DoubleType),
      StructField("LATITUDE", DoubleType),
      StructField("GEORESULT", StringType),
      StructField("CONFIDENCE_CODE", StringType),
      StructField("COUNTRY_ACCESS_CODE", StringType),
      StructField("TEL_NUM", StringType),
      StructField("FAXNUM", StringType),
      StructField("EMAIL", StringType),
      StructField("HTTP", StringType),
      StructField("OPEN_24H", StringType),
      StructField("BUSINESS_LINE", StringType),
      StructField("SIC1", StringType),
      StructField("SIC2", StringType),
      StructField("SIC8", StringType),
      StructField("SIC8_DESCRIPTION", StringType),
      StructField("ALT_INDUSTRY_CODE", StringType),
      StructField("MICODE", StringType),
      StructField("TRADE_DIVISION", StringType),
      StructField("GROUP", StringType),
      StructField("CLASS", StringType),
      StructField("SUB_CLASS", StringType),
      StructField("EMPLOYEE_HERE", StringType),
      StructField("EMPLOYEE_COUNT", StringType),
      StructField("YEAR_START", StringType),
      StructField("SALES_VOLUME_LOCAL", StringType),
      StructField("SALES_VOLUME_US_DOLLARS", StringType),
      StructField("CURRENCY_CODE", StringType),
      StructField("AGENT_CODE", StringType),
      StructField("LEGAL_STATUS_CODE", StringType),
      StructField("STATUS_CODE", StringType),
      StructField("SUBSIDIARY_INDICATOR", StringType),
      StructField("PARENT_BUSINESS_NAME", StringType),
      StructField("PARENT_ADDRESS", StringType),
      StructField("PARENT_STREET_ADDRESS", StringType),
      StructField("PARENT_AREANAME3", StringType),
      StructField("PARENT_AREANAME1", StringType),
      StructField("PARENT_COUNTRY", StringType),
      StructField("PARENT_POSTCODE", StringType),
      StructField("DOMESTIC_ULTIMATE_BUSINESS_NAME", StringType),
      StructField("DOMESTIC_ULTIMATE_ADDRESS", StringType),
      StructField("DOMESTIC_ULTIMATE_STREET_ADDRESS", StringType),
      StructField("DOMESTIC_ULTIMATE_AREANAME3", StringType),
      StructField("DOMESTIC_ULTIMATE_AREANAME1", StringType),
      StructField("DOMESTIC_ULTIMATE_POSTCODE", StringType),
      StructField("GLOBAL_ULTIMATE_INDICATOR", StringType),
      StructField("GLOBAL_ULTIMATE_BUSINESS_NAME", StringType),
      StructField("GLOBAL_ULTIMATE_ADDRESS", StringType),
      StructField("GLOBAL_ULTIMATE_STREET_ADDRESS", StringType),
      StructField("GLOBAL_ULTIMATE_AREANAME3", StringType),
      StructField("GLOBAL_ULTIMATE_AREANAME1", StringType),
      StructField("GLOBAL_ULTIMATE_COUNTRY", StringType),
      StructField("GLOBAL_ULTIMATE_POSTCODE", StringType),
      StructField("FAMILY_MEMBERS", StringType),
      StructField("HIERARCHY_CODE", StringType),
      StructField("TICKER_SYMBOL", StringType),
      StructField("EXCHANGE_NAME", StringType)
    ))
  }
}
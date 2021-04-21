/*
 * Copyright 2019,2021 Precisely
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

import com.pb.downloadmanager.api.DownloadManagerBuilder
import com.pb.downloadmanager.api.downloaders.LocalFilePassthroughDownloader
import com.pb.downloadmanager.api.downloaders.hadoop.{HDFSDownloader, S3Downloader}
import com.precisely.addressing.v1.model.RequestAddress
import com.precisely.bigdata.addressing.spark.api.{AddressingBuilder, AddressingExecutor, RequestInput}
import com.precisely.addressing.v1.model.PreferencesBuilder
import org.apache.commons.collections.CollectionUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{callUDF, col, lit, map}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import java.util.Arrays;

object MultipassAddressing {
  def main(args: Array[String]): Unit = {
    val inputAddressPath = args(0)
    val resourcesLocation = args(1)
    val dataLocation = args(2)
    val downloadLocation = args(3)
    val outputDirectory = args(4)

    val sparkConf = new SparkConf()
    sparkConf.setIfMissing("spark.master", "local[*]")
    val session = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    // Load the addresses from the csv
    val addressInputDF = AddressInput.open(session, inputAddressPath)

    // set preference to return all fields
    val prefBuilder = new PreferencesBuilder().withReturnAllInfo(true).build()

    val downloadManager = new DownloadManagerBuilder(downloadLocation)
      .addDownloader(new S3Downloader(session.sparkContext.hadoopConfiguration))
      .addDownloader(new HDFSDownloader(session.sparkContext.hadoopConfiguration))
      .addDownloader(new LocalFilePassthroughDownloader())
      .build()

    // build a AddressingUDF, with custom executor
    val addressingUdf: UserDefinedFunction = new AddressingBuilder()
      .withResourcesLocation(resourcesLocation)
      .withDataLocations(dataLocation)
      .withDownloadManager(downloadManager)
      .udfBuilder()
      .withPreferences(prefBuilder)
      .withOutputFields(
        "location.feature.geometry.coordinates.x as X",
        "location.feature.geometry.coordinates.y as Y",
        "address.formattedStreetAddress as formattedStreetAddress",
        "address.formattedLocationAddress as formattedLocationAddress",
        "customFields['PRECISION_CODE'] as PRECISIONCODE")
      .withErrorField("error")
      .forCustomExecutor(new CustomExecutor())

    // call UDF for each row in the address DataFrame, this will result in the dataframe containing a new column for each
    // of the specified output fields as well as an error column
    addressInputDF
      // Adds a new column, represented as a collection comprised of the outputFields and the error field
      .withColumn("addressing_geocode_result", addressingUdf(map(
        lit("street"), col("streetName"),
        lit("city"), col("city"),
        lit("admin1"), col("state"),
        lit("postalCode"), col("zip")))
      )
      // Persist the addressing result to avoid recalculation when we expand the result
      .persist()
      // Expand the result collection such that each output field is a separate column, including the error field.
      .select("*", "addressing_geocode_result.*").drop("addressing_geocode_result")
      // Write the dataframe to the specified output folder as parquet file
      .write.mode(SaveMode.Overwrite).option("header","true").csv(outputDirectory)
  }
}

/*
 * A custom Executor that contains the logic to perform verify and then multipass geocode
 */
class CustomExecutor extends com.precisely.bigdata.addressing.spark.api.AddressingExecutor {

  import com.precisely.addressing.v1.model.Response
  import com.precisely.addressing.v1.{Addressing, Preferences}

  var relaxedGeocoderPreferences: Preferences = _

  override def execute(input: RequestInput, preferences: Option[Preferences], addressing: Addressing): Response = {
    if (relaxedGeocoderPreferences == null) {
      relaxedGeocoderPreferences = new PreferencesBuilder()
        .withMatchMode("RELAXED")
        .build()
    }

    val verifyResult = addressing.verify(input.requestAddress(), preferences.orNull).getResults
    val verifyAddress = verifyResult.get(0).getAddress
    
    //  create multiLine request from the result fields obtained from verify api
    val multiLineRequest: RequestAddress = new RequestAddress()
    multiLineRequest.setStreet(verifyAddress.getStreet)
    multiLineRequest.setAddressNumber(verifyAddress.getAddressNumber)
    multiLineRequest.setAdmin1(verifyAddress.getAdmin1.getLongName)
    multiLineRequest.setAdmin2(verifyAddress.getAdmin2.getLongName)
    multiLineRequest.setCountry(verifyAddress.getCountry.getIsoAlpha3Code)
    multiLineRequest.setCity(verifyAddress.getCity.getLongName)
    multiLineRequest.setPostalCode(verifyAddress.getPostalCode)

    val multiLineResponse = addressing.geocode(multiLineRequest, preferences.orNull)
    val multiLineResult = multiLineResponse.getResults

    if (CollectionUtils.isNotEmpty(multiLineResult) && multiLineResult.get(0).getCustomFields
      .get("PRECISION_CODE") != null && Seq("S5", "S6", "S7", "S8").contains(multiLineResult
      .get(0).getCustomFields.get("PRECISION_CODE").slice(0, 2))) {
      return multiLineResponse
    }

    //  create singleline request from multiLine Response
    val singleLine = verifyAddress.getFormattedAddress + " " + verifyAddress.getCountry.getIsoAlpha3Code
    val singleLineRequest: RequestAddress = new RequestAddress();
    singleLineRequest.setAddressLines(Arrays.asList(singleLine))

    val singleLineResult = addressing.geocode(singleLineRequest, relaxedGeocoderPreferences)
    singleLineResult
  }
}

object AddressInput {
  // set these parameters according to the CSV
  def open(session: SparkSession, inputAddressPath: String): DataFrame = {
    session.read
      .option("delimiter", ",")
      .option("header", "false")
      .schema(inputAddressSchema)
      .csv(inputAddressPath)
  }

  private def inputAddressSchema: StructType = {
    // this is the schema for the address fabric data
    StructType(Array(
      StructField("streetName", StringType),
      StructField("city", StringType),
      StructField("state", StringType),
      StructField("zip", StringType)
    ))
  }
}

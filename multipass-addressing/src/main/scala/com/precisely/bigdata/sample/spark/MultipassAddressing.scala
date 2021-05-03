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
import com.precisely.addressing.v1.model.{PreferencesBuilder, RequestAddress}
import com.precisely.bigdata.addressing.spark.api.{AddressingBuilder, AddressingExecutor, RequestInput}
import org.apache.commons.collections.CollectionUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, map}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Arrays


object MultipassAddressing {
  def main(args: Array[String]): Unit = {
    val resourcesLocation = args(0)
    val dataLocation = args(1)
    val downloadLocation = args(2)
    val inputAddressPath = args(3)
    val outputDirectory = args(4)

    val sparkConf = new SparkConf()
    sparkConf.setIfMissing("spark.master", "local[*]")
    val session = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    // Load the addresses from the csv
    val addressInputDF = AddressInput.open(session, inputAddressPath)

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
      .withOutputFields(
        "location.feature.geometry.coordinates.x as X",
        "location.feature.geometry.coordinates.y as Y",
        "address.formattedStreetAddress as formattedStreetAddress",
        "address.formattedLocationAddress as formattedLocationAddress",
        "customFields['PRECISION_CODE'] as precisionCode")
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
      // Write the dataframe to the specified output folder as csv file
      .write.mode(SaveMode.Overwrite).option("header", "true").csv(outputDirectory)
  }
}

/*
 * A custom Executor that contains the logic to perform verify and then multipass geocode
 */
class CustomExecutor extends AddressingExecutor {

  import com.precisely.addressing.v1.model.Response
  import com.precisely.addressing.v1.{Addressing, Preferences}

  // set preference to return all fields and Match Mode to Relaxed
  private val relaxedGeocoderPreferences: Preferences = new PreferencesBuilder()
    .withReturnAllInfo(true)
    .withMatchMode("RELAXED")
    .build()

  override def execute(input: RequestInput, preferences: Option[Preferences], addressing: Addressing): Response = {

    val verifyResult = addressing.verify(input.requestAddress(), preferences.orNull).getResults
    val verifyAddress = verifyResult.get(0).getAddress

    var multiLineRequest: RequestAddress = input.requestAddress()

    var singleLine = input.requestAddress().getStreet + " " +
      input.requestAddress().getCity + " " +
      input.requestAddress().getAdmin1 + " " +
      input.requestAddress().getPostalCode

    if (CollectionUtils.isNotEmpty(verifyResult)) {
      //  create multiLine request from verify Response
      multiLineRequest = new RequestAddress()
      multiLineRequest.setStreet(verifyAddress.getStreet)
      multiLineRequest.setAddressNumber(verifyAddress.getAddressNumber)
      multiLineRequest.setAdmin1(verifyAddress.getAdmin1.getLongName)
      multiLineRequest.setAdmin2(verifyAddress.getAdmin2.getLongName)
      multiLineRequest.setCountry(verifyAddress.getCountry.getIsoAlpha3Code)
      multiLineRequest.setCity(verifyAddress.getCity.getLongName)
      multiLineRequest.setPostalCode(verifyAddress.getPostalCode)

      // create singleline request from verify Response
      singleLine = verifyAddress.getFormattedAddress + " " + verifyAddress.getCountry.getIsoAlpha3Code
    }

    val multiLineResponse = addressing.geocode(multiLineRequest, relaxedGeocoderPreferences)
    val multiLineResult = multiLineResponse.getResults

    //TODO remove multiLineResult.get(0).getLocation.getExplanation.getType != null check once Bug GN-4279 is fixed
    if (CollectionUtils.isNotEmpty(multiLineResult) && multiLineResult.get(0).getLocation.getExplanation.getType != null) {
      if ("ADDRESS_POINT".equals(multiLineResult.get(0).getLocation.getExplanation.getType.label)) {
        return multiLineResponse
      }
    }

    val singleLineRequest: RequestAddress = new RequestAddress()
    singleLineRequest.setAddressLines(Arrays.asList(singleLine))

    val singleLineResponse = addressing.geocode(singleLineRequest, relaxedGeocoderPreferences)
    val singleLineResult = singleLineResponse.getResults

    //TODO remove singleLineResult.get(0).getLocation.getExplanation.getType != null check once Bug GN-4279 is fixed
    if (CollectionUtils.isNotEmpty(singleLineResult) && singleLineResult.get(0).getLocation.getExplanation.getType != null) {
      if ("ADDRESS_POINT".equals(singleLineResult.get(0).getLocation.getExplanation.getType.label)) {
        return singleLineResponse
      }
    }
    if (CollectionUtils.isNotEmpty(singleLineResult) && CollectionUtils.isNotEmpty(multiLineResult)) {
      if (multiLineResult.get(0).getScore > singleLineResult.get(0).getScore) {
        return multiLineResponse
      }
    }

    singleLineResponse
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
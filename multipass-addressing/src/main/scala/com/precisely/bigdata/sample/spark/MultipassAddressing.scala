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
import com.precisely.addressing.v1.model.{PreferencesBuilder, RequestAddress, Result}
import com.precisely.bigdata.addressing.spark.api.{AddressingBuilder, AddressingExecutor, RequestInput}
import org.apache.commons.collections.CollectionUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, map}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.{ArrayList, Collections, List}


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
        lit("addressLines[0]"), col("streetName"),
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

    val verifyResponse = addressing.verify(input.requestAddress(), preferences.orNull)

    val inputRequest: RequestAddress = input.requestAddress()

    var multiLineRequest: RequestAddress = inputRequest

    if (CollectionUtils.isNotEmpty(verifyResponse.getResults)) {
      val verifyAddress = verifyResponse.getResults.get(0).getAddress

      //  create multiLine request from verify Response
      multiLineRequest = new RequestAddress()
      multiLineRequest.setAddressLines(getValidAddressLine(verifyResponse.getResults.get(0).getAddressLines))
      multiLineRequest.setStreet(verifyAddress.getStreet)
      multiLineRequest.setAddressNumber(verifyAddress.getAddressNumber)
      multiLineRequest.setCity(verifyAddress.getCity.getLongName)
      multiLineRequest.setPostalCode(verifyAddress.getPostalCode)
      multiLineRequest.setCountry(verifyAddress.getCountry.getIsoAlpha3Code)

    }

    val multiLineResponse = addressing.geocode(multiLineRequest, relaxedGeocoderPreferences)
    if (CollectionUtils.isNotEmpty(multiLineResponse.getResults) && isAddressLevelMatch(multiLineResponse.getResults.get(0))) {
      return multiLineResponse
    }

    val singleLine: String = if (CollectionUtils.isNotEmpty(verifyResponse.getResults)) {
      for (addressLine <- 0 to getValidAddressLine(verifyResponse.getResults.get(0).getAddressLines).size() - 1) {
        verifyResponse.getResults.get(0).getAddressLines.get(addressLine) + " "
      }
      verifyResponse.getResults.get(0).getAddress.getFormattedAddress + " " + verifyResponse.getResults.get(0).getAddress.getCountry.getIsoAlpha3Code
    } else {
      inputRequest.getStreet + " " +
        inputRequest.getCity + " " +
        inputRequest.getAdmin1 + " " +
        inputRequest.getPostalCode
    }

    val singleLineRequest: RequestAddress = new RequestAddress()
    singleLineRequest.setAddressLines(Collections.singletonList(singleLine))

    val singleLineResponse = addressing.geocode(singleLineRequest, relaxedGeocoderPreferences)
    if (CollectionUtils.isNotEmpty(singleLineResponse.getResults) && isAddressLevelMatch(singleLineResponse.getResults.get(0))) {
      return singleLineResponse
    }

    if (CollectionUtils.isNotEmpty(multiLineResponse.getResults)) {
      if (CollectionUtils.isNotEmpty(singleLineResponse.getResults)
        && singleLineResponse.getResults.get(0).getScore >= multiLineResponse.getResults.get(0).getScore) {
        return singleLineResponse
      }
      multiLineResponse
    } else {
      singleLineResponse
    }
  }

  def isAddressLevelMatch(result: Result): Boolean = {
    if ("ADDRESS".equals(result.getExplanation.getAddressMatch.getType.label) && result.getScore >= 90) {
      return true
    }
    false
  }

  def getValidAddressLine(addressLines: List[String]): List[String] = {
    val blankLines = new ArrayList[String]()
    blankLines.add("")
    addressLines.removeAll(blankLines)
    if (addressLines.size() > 1) {
      addressLines.subList(0, addressLines.size() - 1)
    }
    addressLines
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
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
import scala.collection.JavaConversions._


object MultipassAddressing {
  def main(args: Array[String]): Unit = {
    val resourcesLocation = args(0)
    val dataLocation = args(1)
    val downloadLocation = args(2)
    val inputAddressPath = args(3)
    val outputDirectory = args(4)
    val sparkConf = new SparkConf()
    sparkConf.setIfMissing("spark.master", "local[*]")
    var sparkMajorVersion=org.apache.spark.SPARK_VERSION.split('=')(0).split('.')(0).toInt
    if(sparkMajorVersion >= 3){
      // Set this variable only for Spark version 3.0.x only
      var sparkMinorVersion=org.apache.spark.SPARK_VERSION.split('=')(0).split('.')(1).toInt
      if(sparkMajorVersion == 3 and sparkMinorVersion == 0){
        sparkConf.setIfMissing("spark.sql.legacy.allowUntypedScalaUDF","true")
      }

    }
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
        lit("postalCode"), col("zip"),
        lit("country"), lit("USA")))
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

    // get the request address represented by this record in the dataframe
    val inputRequest: RequestAddress = input.requestAddress()

    // perform the verify operation
    val verifyResponse = addressing.verify(inputRequest, preferences.orNull)

    // default input for the initial geocode should be the original user specified address to handle the case where verify returns no result
    var multiLineRequest: RequestAddress = inputRequest

    if (CollectionUtils.isNotEmpty(verifyResponse.getResults)) {
      val verifyAddress = verifyResponse.getResults.get(0).getAddress
      //  create multiLine request from verify Response
      multiLineRequest = new RequestAddress()
      multiLineRequest.setAddressLines(getMainAddressLines(verifyResponse.getResults.get(0).getAddressLines))
      multiLineRequest.setAdmin1(verifyAddress.getAdmin1.getLongName)
      multiLineRequest.setAdmin2(verifyAddress.getAdmin2.getLongName)
      multiLineRequest.setCity(verifyAddress.getCity.getLongName)
      multiLineRequest.setPostalCode(verifyAddress.getPostalCode)
      multiLineRequest.setCountry(verifyAddress.getCountry.getIsoAlpha3Code)

    }

    // Perform the geocode and return the result if it passes our acceptance criteria
    val multiLineResponse = addressing.geocode(multiLineRequest, relaxedGeocoderPreferences)
    if (CollectionUtils.isNotEmpty(multiLineResponse.getResults) && isAddressLevelMatch(multiLineResponse.getResults.get(0))) {
      return multiLineResponse
    }

    // build a single line version of the address for the second pass of geocoding, based on the verified address or the input
    val singleLineRequest: RequestAddress = new RequestAddress()
    if (CollectionUtils.isNotEmpty(verifyResponse.getResults)) {
      val verifyAddress = verifyResponse.getResults.get(0).getAddress
      singleLineRequest.setAddressLines(Collections.singletonList(verifyAddress.getFormattedAddress))
      singleLineRequest.setCountry(verifyAddress.getCountry.getIsoAlpha3Code)
    } else {
      singleLineRequest.setAddressLines(Collections.singletonList(
        inputRequest.getStreet + " " +
          inputRequest.getCity + " " +
          inputRequest.getAdmin1 + " " +
          inputRequest.getPostalCode
      ))
      singleLineRequest.setCountry(inputRequest.getCountry)
    }

    // Perform the single line geocode and return the result if it passes our acceptance criteria
    val singleLineResponse = addressing.geocode(singleLineRequest, relaxedGeocoderPreferences)
    if (CollectionUtils.isNotEmpty(singleLineResponse.getResults) && isAddressLevelMatch(singleLineResponse.getResults.get(0))) {
      return singleLineResponse
    }

    // neither address passed our initial acceptance criteria... pick the best response
    if (CollectionUtils.isNotEmpty(multiLineResponse.getResults)) {
      // Return the single line response only if it is better than the multi line response
      if (CollectionUtils.isNotEmpty(singleLineResponse.getResults)
        && singleLineResponse.getResults.get(0).getScore > multiLineResponse.getResults.get(0).getScore) {
        return singleLineResponse
      }
      multiLineResponse
    } else {
      singleLineResponse
    }
  }

  def isAddressLevelMatch(result: Result): Boolean =
    result.getScore >= 90 && "ADDRESS".equals(result.getExplanation.getAddressMatch.getType.label)

  def getMainAddressLines(addressLines: List[String]): List[String] = {
    val mainAddressLine = new ArrayList[String]()
    mainAddressLine.addAll(addressLines.filter(_.nonEmpty).dropRight(1))
    mainAddressLine
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

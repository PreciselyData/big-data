/*
 * Copyright 2017, 2021 Precisely. All rights reserved.
 * This document contains unpublished, confidential, and proprietary information of Precisely.
 * No disclosure or use of any portion of the contents of this document may be made without the express written consent of Precisely.
 */

package com.precisely.bigdata.addressing.spark.app

import com.pb.downloadmanager.api.downloaders.LocalFilePassthroughDownloader
import com.pb.downloadmanager.api.downloaders.hadoop.{AzureDownloader, GoogleDownloader, HDFSDownloader, S3Downloader}
import com.pb.downloadmanager.api.{DownloadManagerBuilder, PermissionsManager, PosixPermissionsManagerBuilder}
import com.precisely.bigdata.addressing.spark.api.{AddressingBuilder, UDFBuilder}
import com.precisely.bigdata.addressing.spark.app.DriverUtils.{GEOCODE, LOOKUP, REVERSE, REVERSE_GEOCODE, VERIFY, buildInputAddressingFields, buildInputLookupFields, buildInputReverseGeocodeFields}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, SaveMode, SparkSession}

object SampleAddressingDriver extends App {
  val commandLine = new BaseCommandLine(args.toSeq, this.getClass.getName, "command line for addressing (geocode, verify, lookup, reverseGeocode)")
  private val session = SparkSession.builder().appName(this.getClass.getName).getOrCreate()

  var df = session.read.options(commandLine.getInputOptions).format(commandLine.inputFormat.toOption.get.toString)
    .load(commandLine.input())

  if (commandLine.limit.isDefined) {
    df = df.limit(commandLine.limit())
  }

  if (commandLine.numPartitions.isDefined) {
    df = df.repartition(commandLine.numPartitions())
  }

  var builder = new AddressingBuilder()
    .withResourcesLocation(commandLine.resourcesLocation())

  if (commandLine.downloadLocation.isDefined) {
    var permissionsManager: PermissionsManager = null

    if (commandLine.downloadGroup.isDefined) {
      permissionsManager = new PosixPermissionsManagerBuilder().withGroup(commandLine.downloadGroup()).build()
    }

    val downloadManager = new DownloadManagerBuilder(commandLine.downloadLocation(), permissionsManager)
      .addDownloader(new S3Downloader(session.sparkContext.hadoopConfiguration))
      .addDownloader(new GoogleDownloader(session.sparkContext.hadoopConfiguration))
      .addDownloader(new AzureDownloader(session.sparkContext.hadoopConfiguration))
      .addDownloader(new HDFSDownloader(session.sparkContext.hadoopConfiguration))
      .addDownloader(new LocalFilePassthroughDownloader())
      .build()

    builder = builder.withDownloadManager(downloadManager)
  }

  commandLine.extractionLocation.foreach(builder.withExtractionLocation(_))
  commandLine.dataLocation.foreach(builder.withDataLocations(_: _*))

  private val udfBuilder: UDFBuilder = builder.udfBuilder().withOutputFields(commandLine.outputFields(): _*)

  commandLine.preferencesFilepath.foreach(udfBuilder.withPreferencesFile)
  commandLine.errorField.foreach(udfBuilder.withErrorField)
  commandLine.jsonOutputField.foreach(udfBuilder.withResultAsJSON)

  private val operationUdf: UserDefinedFunction = commandLine.operation().toLowerCase() match {
    case GEOCODE =>
      udfBuilder.forGeocode()
    case VERIFY =>
      udfBuilder.forVerify()
    case LOOKUP =>
      udfBuilder.forLookup()
    case REVERSE_GEOCODE | REVERSE =>
      udfBuilder.forReverseGeocode()
    case _ =>
      throw new IllegalArgumentException("Not a valid '--operation' parameter")
  }

  private val datasetName = "addressing_result"

  commandLine.operation().toLowerCase() match {
    case GEOCODE | VERIFY =>
      val mapOfAddressingFields: Column = buildInputAddressingFields(commandLine, df)
      df = df.withColumn(datasetName, operationUdf(mapOfAddressingFields))
        .persist()
        .select("*", datasetName + ".*").drop(datasetName)
    case LOOKUP =>
      val (col1, col2, col3) = buildInputLookupFields(commandLine, df)
      df = df.withColumn(datasetName, operationUdf(col1, col2, col3))
        .persist()
        .select("*", datasetName + ".*").drop(datasetName)
    case REVERSE_GEOCODE | REVERSE =>
      val (x, y, country) = buildInputReverseGeocodeFields(commandLine, df)
      df = df.withColumn(datasetName, operationUdf(x, y, country))
        .persist()
        .select("*", datasetName + ".*").drop(datasetName)
    case _ =>
      throw new IllegalArgumentException("Not a valid '--operation' parameter")
  }

  if (commandLine.combine())
    df = df.repartition(1)

  private val saveMode = if (commandLine.overwrite()) SaveMode.Overwrite else SaveMode.ErrorIfExists

  df.write.mode(saveMode).options(commandLine.getOutputOptions).format(commandLine.outputFormat.toOption.get.toString)
    .save(commandLine.output())
}

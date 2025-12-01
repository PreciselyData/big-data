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
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes.DoubleType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SampleAddressingDriver extends App {
  private val buildInputAddressMap = (commandLine: BaseCommandLine, df: DataFrame) => {
    val addressFields = commandLine.inputFields.filterNot(_._1 == COUNTRY_KEY)
      .map(inputField =>
        (lit(inputField._1), concat_ws(" ", inputField._2.split(",").toSeq.map(field =>
          col(df.columns(DriverUtils.getRequiredColumnIndex(inputField._1, Option(field), df)))
        ): _*))
      )
      .flatMap { case (k, v) => Seq(k, v) }
      .toList

    val countryKey = lit("country")
    val countryValue = DriverUtils.buildFallbackToLiteral("country", commandLine.country, df, commandLine)

    map((addressFields :+ countryKey :+ countryValue).toIndexedSeq: _*)
  }

  private val COUNTRY_KEY = "country"
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

  private var builder = new AddressingBuilder()
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

  private val operationUdf: UserDefinedFunction = commandLine.operation() match {
    case "geocode" =>
      udfBuilder.forGeocode()
    case "verify" =>
      udfBuilder.forVerify()
    case "lookup" =>
      udfBuilder.forLookup()
    case "reverseGeocode" =>
      udfBuilder.forReverseGeocode()
    case _ =>
      throw new IllegalArgumentException("Not a valid '--operation' parameter")
  }

  private val datasetName = "addressing_result"

  commandLine.operation() match {
    case "geocode" | "verify" | "parse" =>
      df = df.withColumn(datasetName, operationUdf(buildInputAddressMap(commandLine, df)))
        .persist()
        .select("*", datasetName + ".*").drop(datasetName)
    case "lookup" =>
      df = df.withColumn(datasetName, operationUdf(lit("PB_KEY"), col(df.columns(Integer.valueOf(commandLine.inputFields("key")))), lit("USA")))
        .persist()
        .select("*", datasetName + ".*").drop(datasetName)
    case "reverseGeocode" =>
      if (commandLine.inputFields.contains("country"))
        df = df.withColumn(datasetName, operationUdf(col(df.columns(Integer.valueOf(commandLine.inputFields("x")))).cast(DoubleType), col(df.columns(Integer.valueOf(commandLine.inputFields("y")))).cast(DoubleType), col(df.columns(Integer.valueOf(commandLine.inputFields("country"))))))
          .persist()
          .select("*", datasetName + ".*").drop(datasetName)
      else
        df = df.withColumn(datasetName, operationUdf(col(df.columns(Integer.valueOf(commandLine.inputFields("x")))).cast(DoubleType), col(df.columns(Integer.valueOf(commandLine.inputFields("y")))).cast(DoubleType), lit("")))
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

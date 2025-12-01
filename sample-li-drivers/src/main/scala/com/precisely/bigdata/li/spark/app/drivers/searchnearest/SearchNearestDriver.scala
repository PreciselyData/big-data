/*
 * Copyright 2017, 2025 Precisely. All rights reserved.
 * This document contains unpublished, confidential, and proprietary information of Precisely.
 * No disclosure or use of any portion of the contents of this document may be made without the express written consent of Precisely.
*/

package com.precisely.bigdata.li.spark.app.drivers.searchnearest

import com.pb.downloadmanager.api.downloaders.LocalFilePassthroughDownloader
import com.pb.downloadmanager.api.downloaders.hadoop.{GoogleDownloader, HDFSDownloader, S3Downloader}
import com.pb.downloadmanager.api.{DownloadManagerBuilder, PosixPermissionsManagerBuilder}
import com.precisely.bigdata.li.spark.api.SpatialAPI
import com.precisely.bigdata.li.spark.app.drivers.DriverConstants.AllowUntypedScalaUDF
import com.precisely.bigdata.li.spark.app.drivers.searchnearest
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SearchNearestDriver extends App {
    val commandLine = new searchnearest.BaseCommandLine(args.toSeq, this.getClass.getName, "command line for searchNearest operation")
    val spark = SparkSession.builder().appName(this.getClass.getName).config(AllowUntypedScalaUDF, "true").getOrCreate()

    val inputDF: DataFrame = spark.read.options(commandLine.getInputOptions).format(commandLine.inputFormat.toOption.get.toString)
      .load(commandLine.input())

    val fabricDF: DataFrame = if (commandLine.limit.isDefined) inputDF.limit(commandLine.limit()) else inputDF

    val tableFileType = commandLine.tableFileType()
    val tableFilePath = commandLine.tableFilePath()
    val tableFileName = commandLine.tableFileName()
    val distanceValue = commandLine.distance().doubleValue()
    val distanceUnit = commandLine.distanceUnit()
    val maxCandidates = commandLine.maxCandidates().intValue()
    val distanceColumnName = commandLine.distanceColumnName()
    val outputFields: Seq[String] = commandLine.outputFields()
    val geometryStringType = commandLine.geometryStringType()
    val saveMode = if (commandLine.overwrite()) SaveMode.Overwrite else SaveMode.ErrorIfExists
    SpatialAPI.searchNearest(inputDF = fabricDF, tableFileType = tableFileType, tableFilePath = tableFilePath, tableFileName = tableFileName,
        libraries = if (commandLine.libraries.isDefined) commandLine.libraries.toString() else null,
        maxCandidates = maxCandidates,
        distanceValue = distanceValue, distanceUnit = distanceUnit, distanceColumnName = distanceColumnName,
        geometryStringType = geometryStringType, geometryColumnName = commandLine.geometryColumnName(),
        includeEmptySearchResults = commandLine.includeEmptySearchResults(), outputFields = outputFields,
        downloadManager = if (commandLine.downloadLocation.isDefined)
          new DownloadManagerBuilder(commandLine.downloadLocation(), if (commandLine.downloadGroup.isDefined) new PosixPermissionsManagerBuilder().withGroup(commandLine.downloadGroup()).build() else null)
            .addDownloader(new S3Downloader(spark.sparkContext.hadoopConfiguration))
            .addDownloader(new GoogleDownloader(spark.sparkContext.hadoopConfiguration))
            .addDownloader(new HDFSDownloader(spark.sparkContext.hadoopConfiguration))
            .addDownloader(new LocalFilePassthroughDownloader())
            .build()
        else null)
      .write.mode(saveMode).options(commandLine.getOutputOptions).format(commandLine.outputFormat.toOption.get.toString)
      .save(commandLine.output())
}

/*
 * Copyright 2017, 2025 Precisely. All rights reserved.
 * This document contains unpublished, confidential, and proprietary information of Precisely.
 * No disclosure or use of any portion of the contents of this document may be made without the express written consent of Precisely.
 */

package com.precisely.bigdata.li.spark.app.drivers.hexgen

import com.precisely.bigdata.li.spark.api.SpatialAPI
import com.precisely.bigdata.li.spark.app.drivers.DriverConstants.AllowUntypedScalaUDF
import com.precisely.bigdata.li.spark.app.drivers.hexgen
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SampleHexGenLIDriver extends App {
    val commandLine = new hexgen.BaseCommandLine(args.toSeq, this.getClass.getName, "command line for hexgen operation")

    val session = SparkSession.builder().appName(this.getClass.getName).config(AllowUntypedScalaUDF, "true").getOrCreate()

    val minLongitude: Double = commandLine.minLongitude()
    val minLatitude: Double = commandLine.minLatitude()
    val maxLongitude: Double = commandLine.maxLongitude()
    val maxLatitude: Double = commandLine.maxLatitude()
    val hexLevel: Int = commandLine.hexLevel()
    val containerLevel: Int = commandLine.containerLevel()
    val numOfPartitions: Int = commandLine.numberOfPartitions()
    val maxNumberOfRows: Int = commandLine.maxNumberOfRows()

    val df: DataFrame = SpatialAPI.generateHexagon(sparkSession = session, minLongitude = minLongitude, minLatitude = minLatitude,
      maxLongitude = maxLongitude, maxLatitude = maxLatitude,
      hexLevel = hexLevel, containerLevel = containerLevel,
      numOfPartitions = numOfPartitions, maximumNumOfRowsPerPartition = maxNumberOfRows)

    val saveMode = if (commandLine.overwrite()) SaveMode.Overwrite else SaveMode.ErrorIfExists
    df.write.mode(saveMode).options(commandLine.getOutputOptions).format(commandLine.outputFormat.toOption.get.toString)
      .save(commandLine.output())
}

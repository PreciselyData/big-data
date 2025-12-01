/*
 * Copyright 2017, 2025 Precisely. All rights reserved.
 * This document contains unpublished, confidential, and proprietary information of Precisely.
 * No disclosure or use of any portion of the contents of this document may be made without the express written consent of Precisely.
 */

package com.precisely.bigdata.li.spark.app.drivers.joinbydistance

import com.precisely.bigdata.li.spark.api.SpatialAPI
import com.precisely.bigdata.li.spark.api.util.DistanceJoinOption
import com.precisely.bigdata.li.spark.api.util.DistanceJoinOption.DistanceJoinOption
import com.precisely.bigdata.li.spark.api.util.LimitMethods.LimitMethods
import com.precisely.bigdata.li.spark.app.drivers.DriverConstants.AllowUntypedScalaUDF
import com.precisely.bigdata.li.spark.app.drivers.joinbydistance
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.rogach.scallop.ScallopOption

object JoinByDistanceDriver extends App {

    private def getJoinOptions(limit: ScallopOption[Int], distanceColumnName: ScallopOption[String],
                               limitMethod: LimitMethods): Map[DistanceJoinOption, Any] = {
      var options = Map.empty[DistanceJoinOption, Any]
      if (distanceColumnName.isDefined)
        options += (DistanceJoinOption.DistanceColumnName -> distanceColumnName())
      if (limit.isDefined)
        options += (DistanceJoinOption.LimitMatches -> limit())
      if (limitMethod != null)
        options += (DistanceJoinOption.LimitMethod -> limitMethod)
      options
    }

    val commandLine = new joinbydistance.BaseCommandLine(args.toSeq, this.getClass.getName, "command line for joinByDistance operation")

    val session = SparkSession.builder().appName(this.getClass.getName).config(AllowUntypedScalaUDF, "true").getOrCreate()

    val searchRadius: Double = commandLine.searchRadius().doubleValue()
    val distanceUnit: String = commandLine.distanceUnit()
    val longitude1: String = commandLine.longitude1()
    val longitude2: String = commandLine.longitude2()
    val latitude1: String = commandLine.latitude1()
    val latitude2: String = commandLine.latitude2()
    val limitMatches: ScallopOption[Int] = commandLine.limitMatches
    val geoHashPrecision: Int = commandLine.geoHashPrecision.getOrElse(7)
    val distanceColumnName: ScallopOption[String] = commandLine.distanceColumnName
    val limitMethod: LimitMethods = if (commandLine.limitMethod.isDefined) commandLine.getLimitMethodName else null

    val dfUSAPoi = session.read.options(commandLine.getInputOptions1).format(commandLine.inputFormat1.toOption.get.toString)
      .load(commandLine.input1())

    val dfUSAddressFabric = session.read.options(commandLine.getInputOptions2).format(commandLine.inputFormat2.toOption.get.toString)
      .load(commandLine.input2())

    val df: DataFrame = SpatialAPI.joinByDistance(df1 = dfUSAPoi, df2 = dfUSAddressFabric,
      df1Longitude = longitude1, df1Latitude = latitude1, df2Longitude = longitude2, df2Latitude = latitude2,
      searchRadius = searchRadius, distanceUnit = distanceUnit,
      geoHashPrecision = geoHashPrecision, options = getJoinOptions(limitMatches, distanceColumnName, limitMethod))

    val saveMode = if (commandLine.overwrite()) SaveMode.Overwrite else SaveMode.ErrorIfExists
    df.write.mode(saveMode).options(commandLine.getOutputOptions).format(commandLine.outputFormat.toOption.get.toString)
      .save(commandLine.output())
  }


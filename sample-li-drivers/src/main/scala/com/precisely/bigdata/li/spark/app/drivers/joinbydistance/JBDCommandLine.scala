/*
 * Copyright 2017, 2025 Precisely. All rights reserved.
 * This document contains unpublished, confidential, and proprietary information of Precisely.
 * No disclosure or use of any portion of the contents of this document may be made without the express written consent of Precisely.
 */

package com.precisely.bigdata.li.spark.app.drivers.joinbydistance

import com.precisely.bigdata.li.spark.api.util.LimitMethods
import com.precisely.bigdata.li.spark.api.util.LimitMethods.LimitMethods
import com.precisely.bigdata.li.spark.app.drivers.joinbydistance.DataFormat.DataFormat
import org.apache.commons.text.StringEscapeUtils
import org.rogach.scallop.exceptions.{Help, Version}
import org.rogach.scallop.{ArgType, ScallopConf, ScallopOption, ValueConverter, singleArgConverter}

class BaseCommandLine(arguments: Seq[String], driverClass: String, inputFieldsHelpAddition: String) extends ScallopConf(arguments) {
  version(inputFieldsHelpAddition + "\n" + "spark-submit --class " + driverClass + " --master yarn --deploy-mode cluster [SPARK_SUBMIT_OPTIONS] <JAR_PATH> <JOB_OPTIONS>")

  val searchRadius: ScallopOption[Double] = opt[Double](descr = "The absolute value of buffer length around point 1 to search for point 2", required = true, noshort = true)
  val distanceUnit: ScallopOption[String] = opt[String](descr = "Unit of measurement for search-radius parameter", required = true, noshort = true)
  val longitude1: ScallopOption[String] = opt[String](descr = "The longitude column name of the first dataframe", required = true, noshort = true)
  val longitude2: ScallopOption[String] = opt[String](descr = "The longitude column name of the second dataframe", required = true, noshort = true)
  val latitude1: ScallopOption[String] = opt[String](descr = "The latitude column name of the first dataframe", required = true, noshort = true)
  val latitude2: ScallopOption[String] = opt[String](descr = "The latitude column name of the second dataframe", required = true, noshort = true)
  val input1: ScallopOption[String] = opt[String](descr = "The path to the input directory", required = true, noshort = true)
  val input2: ScallopOption[String] = opt[String](descr = "The path to the input directory", required = true, noshort = true)
  val inputFormat1: ScallopOption[DataFormat] = opt[DataFormat](descr = "Specify the Input File Format", noshort = true, default = Some(DataFormat.CSV))(singleArgConverter((arg: String) => DataFormat.withName(arg.toUpperCase)))
  val inputFormat2: ScallopOption[DataFormat] = opt[DataFormat](descr = "Specify the Input File Format", noshort = true, default = Some(DataFormat.CSV))(singleArgConverter((arg: String) => DataFormat.withName(arg.toUpperCase)))
  val csv1: Map[String, String] = propsLong[String](name = "csv1", descr = "Options to control csv1 read/write.  Some values may need to be escaped on the command line, such as delimiter='\\t'")(new CommaAllowedConverter)
  val csv2: Map[String, String] = propsLong[String](name = "csv2", descr = "Options to control csv2 read/write.  Some values may need to be escaped on the command line, such as delimiter='\\t'")(new CommaAllowedConverter)
  val parquet1: Map[String, String] = propsLong[String](name = "parquet1", descr = "Options to control parquet read/write")(new CommaAllowedConverter)
  val parquet2: Map[String, String] = propsLong[String](name = "parquet2", descr = "Options to control parquet read/write")(new CommaAllowedConverter)
  val output: ScallopOption[String] = opt[String](descr = "The HDFS path to the output directory", required = true, noshort = true)
  val outputFormat: ScallopOption[DataFormat] = opt[DataFormat](descr = "Specify the Output File Format", noshort = true, default = Some(DataFormat.CSV))(singleArgConverter((arg: String) => DataFormat.withName(arg.toUpperCase)))
  val csv: Map[String, String] = propsLong[String](name = "csv", descr = "Options to control csv read/write.  Some values may need to be escaped on the command line, such as delimiter='\\t'")(new CommaAllowedConverter)
  val parquet: Map[String, String] = propsLong[String](name = "parquet", descr = "Options to control parquet read/write")(new CommaAllowedConverter)
  val limitMatches: ScallopOption[Int] = opt[Int](descr = "Limit Value as the DistanceJoinOption", noshort = true)
  val geoHashPrecision: ScallopOption[Int] = opt[Int](descr = "The geohash precision", noshort = true)
  val distanceColumnName: ScallopOption[String] = opt[String](descr = "The output distance column name", noshort = true)
  val limitMethod: ScallopOption[LimitMethods] = opt[LimitMethods](descr = "The limit method name", noshort = true)(singleArgConverter((arg: String) => LimitMethods.withName(arg)))
  val overwrite: ScallopOption[Boolean] = opt[Boolean](descr = "Overwrite existing output", noshort = true)

  /**
   * processes File values for escaped characters
   */
  private def getProcessedOptions(options: Map[String, String]): Map[String, String] = {
    options.map { case (k, v) => (k, StringEscapeUtils.unescapeJava(v)) }
  }

  def getLimitMethodName: LimitMethods = {
    limitMethod.toOption match {
      case Some(LimitMethods.RowNumber) => LimitMethods.RowNumber
      case Some(LimitMethods.Rank) => LimitMethods.Rank
      case Some(LimitMethods.DenseRank) => LimitMethods.DenseRank
      case _ => throw new IllegalArgumentException("Only RowNumber, Rank and DenseRank are allowed")
    }
  }

  def getInputOptions1: Map[String, String] = {
    inputFormat1.toOption match {
      case Some(DataFormat.CSV) => getProcessedOptions(csv1)
      case Some(DataFormat.PARQUET) => getProcessedOptions(parquet1)
      case _ => throw new IllegalArgumentException("Only CSV and PARQUET are allowed")
    }
  }

  def getInputOptions2: Map[String, String] = {
    inputFormat2.toOption match {
      case Some(DataFormat.CSV) => getProcessedOptions(csv2)
      case Some(DataFormat.PARQUET) => getProcessedOptions(parquet2)
      case _ => throw new IllegalArgumentException("Only CSV and PARQUET are allowed")
    }
  }

  def getOutputOptions: Map[String, String] = {
    outputFormat.toOption match {
      case Some(DataFormat.CSV) => getProcessedOptions(csv)
      case Some(DataFormat.PARQUET) => getProcessedOptions(parquet)
      case _ => throw new IllegalArgumentException("Only CSV and PARQUET are allowed")
    }
  }

  //we want to print usage out on general error
  override def onError(e: Throwable): Unit = e match {
    case Version =>
      super.onError(e)
    case Help("") =>
      super.onError(e)
    case _ =>
      printHelp()
      super.onError(e)
  }

  verify()
}

//This custom converter allows comma to be embedded without escaping, and is limited to string values.
private class CommaAllowedConverter extends ValueConverter[Map[String, String]] {
  override val argType: ArgType.V = org.rogach.scallop.ArgType.LIST

  override def parse(s: List[(String, List[String])]): Either[String, Option[Map[String, String]]] = {
    try {
      Right {
        val pairs = s.flatMap(_._2).map(_.trim)
        val m = pairs.map { pair =>
          val kv = pair.split("(?<!\\\\)=").map(_.replace("\\=", "="))
          (kv(0), kv(1))
        }.toMap

        if (m.nonEmpty) Some(m)
        else None
      }
    } catch {
      case _: Exception =>
        Left("wrong arguments format")
    }
  }
}

object DataFormat extends Enumeration {
  type DataFormat = Value
  val CSV, PARQUET = Value
}


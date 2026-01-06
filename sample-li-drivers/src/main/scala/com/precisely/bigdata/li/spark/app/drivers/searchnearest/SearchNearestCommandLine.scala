/*
 * Copyright 2017, 2025 Precisely. All rights reserved.
 * This document contains unpublished, confidential, and proprietary information of Precisely.
 * No disclosure or use of any portion of the contents of this document may be made without the express written consent of Precisely.
 */

package com.precisely.bigdata.li.spark.app.drivers.searchnearest

import com.precisely.bigdata.li.spark.app.drivers.searchnearest.DataFormat.DataFormat
import org.apache.commons.text.StringEscapeUtils
import org.rogach.scallop.exceptions.{Help, Version}
import org.rogach.scallop.{ArgType, ScallopConf, ScallopOption, Serialization, ValueConverter, singleArgConverter}

class BaseCommandLine(arguments: Seq[String], driverClass: String, inputFieldsHelpAddition: String) extends ScallopConf(arguments) with Serialization {
  version(inputFieldsHelpAddition + "\n" + "spark-submit --class " + driverClass + " --master yarn --deploy-mode cluster [SPARK_SUBMIT_OPTIONS] <JAR_PATH> <JOB_OPTIONS>")

  val input: ScallopOption[String] = opt[String](descr = "The HDFS path to input file", required = true, noshort = true)
  val inputFormat: ScallopOption[DataFormat] = opt[DataFormat](descr = "Specify the Input File Format", noshort = true, default = Some(DataFormat.CSV))(singleArgConverter((arg: String) => DataFormat.withName(arg.toUpperCase)))
  val tableFileType: ScallopOption[String] = choice(descr = "Type of spatial data provided", required = true, noshort = true, choices = Seq[String]("TAB", "shape", "geodatabase"))
  val tableFilePath: ScallopOption[String] = opt[String](descr = "The HDFS path to spatial table", required = true, noshort = true)
  val tableFileName: ScallopOption[String] = opt[String](descr = "Spatial table file name", required = true, noshort = true)
  val geometryStringType: ScallopOption[String] = choice(descr = "Type of geometry string data provided", required = true, noshort = true, choices = Seq[String]("WKT", "GeoJSON", "KML", "WKB"))
  val geometryColumnName: ScallopOption[String] = opt[String](descr = "Geometry column name for input data", required = true, noshort = true)
  val distanceColumnName: ScallopOption[String] = opt[String](descr = "Distance column name in output", noshort = true, default = Some("distance"))
  val maxCandidates: ScallopOption[Int] = opt[Int](descr = "Maximum number of candidates", noshort = true, required = true)
  val distance: ScallopOption[Double] = opt[Double](descr = "The maximum absolute value of search distance in which target geometries needs to be searched", required = true, noshort = true)
  val distanceUnit: ScallopOption[String] = opt[String](descr = "Unit of measurement for distance parameter", required = true, noshort = true)
  val output: ScallopOption[String] = opt[String](descr = "The HDFS path to the output directory", required = true, noshort = true)
  val outputFormat: ScallopOption[DataFormat] = opt[DataFormat](descr = "Specify the Output File Format", noshort = true, default = inputFormat.toOption)(singleArgConverter((arg: String) => DataFormat.withName(arg.toUpperCase)))
  val outputFields: ScallopOption[List[String]] = opt[List[String]](descr = "Fields from the polygon table to include in the output", required = true, noshort = true)
  val includeEmptySearchResults: ScallopOption[Boolean] = opt[Boolean](descr = "If true then an empty search will keep the original input row and the new columns will be null and if false then an empty search will result in the row not appearing in the outputted DataFrame", noshort = true)
  val libraries: ScallopOption[String] = opt[String](descr = "Libraries in case of geodatabase table-file-type parameter", noshort = true)
  val csv: Map[String, String] = propsLong[String](name = "csv", descr = "Options to control csv read/write.  Some values may need to be escaped on the command line, such as delimiter='\\t'")(new CommaAllowedConverter)
  val parquet: Map[String, String] = propsLong[String](name = "parquet", descr = "Options to control parquet read/write ")(new CommaAllowedConverter)
  val limit: ScallopOption[Int] = opt[Int](descr = "Limit output records", noshort = true)
  val overwrite: ScallopOption[Boolean] = opt[Boolean](descr = "Overwrite existing output", noshort = true)
  val downloadLocation: ScallopOption[String] = opt[String](descr = "Location of the local directory where data will be downloaded to", noshort = true)
  val downloadGroup: ScallopOption[String] = opt[String](descr = "Group to grant permissions for download location", noshort = true)

  /**
   * processes File values for escaped characters
   */
  private def getProcessedOptions(options: Map[String, String]): Map[String, String] = {
    options.map { case (k, v) => (k, StringEscapeUtils.unescapeJava(v)) }
  }

  def getInputOptions: Map[String, String] = {
    inputFormat.toOption match {
      case Some(DataFormat.CSV) => getProcessedOptions(csv)
      case Some(DataFormat.PARQUET) => getProcessedOptions(parquet)
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



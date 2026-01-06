/*
 * Copyright 2017, 2025 Precisely. All rights reserved.
 * This document contains unpublished, confidential, and proprietary information of Precisely.
 * No disclosure or use of any portion of the contents of this document may be made without the express written consent of Precisely.
 */

package com.precisely.bigdata.li.spark.app.drivers.hexgen

import com.precisely.bigdata.li.spark.app.drivers.hexgen.DataFormat.{CSV, DataFormat}
import org.apache.commons.text.StringEscapeUtils
import org.rogach.scallop.exceptions.{Help, Version}
import org.rogach.scallop.{ArgType, ScallopConf, ScallopOption, ValueConverter, singleArgConverter}

class BaseCommandLine(arguments: Seq[String], driverClass: String, inputFieldsHelpAddition: String) extends ScallopConf(arguments) {
  version(inputFieldsHelpAddition + "\n" + "spark-submit --class " + driverClass + " --master yarn --deploy-mode cluster [SPARK_SUBMIT_OPTIONS] <JAR_PATH> <JOB_OPTIONS>")

  val minLongitude: ScallopOption[Double] = opt[Double](descr = "The bottom left longitude of the bounding box.", required = true, noshort = true)
  val minLatitude: ScallopOption[Double] = opt[Double](descr = "The bottom left latitude of the bounding box.", required = true, noshort = true)
  val maxLongitude: ScallopOption[Double] = opt[Double](descr = "The upper right longitude of the bounding box.", required = true, noshort = true)
  val maxLatitude: ScallopOption[Double] = opt[Double](descr = "The upper right latitude of the bounding box.", required = true, noshort = true)
  val hexLevel: ScallopOption[Int] = opt[Int](descr = "The level to generate hexagons for. Must be between 1 and 11.", required = true, noshort = true)
  val containerLevel: ScallopOption[Int] = opt[Int](descr = "A hint for providing some parallel hexagon generation. Must be less than the hexLevel property.", required = true, noshort = true)
  val numberOfPartitions: ScallopOption[Int] = opt[Int](descr = "Number of partitions", noshort = true, default = Some(1))
  val maxNumberOfRows: ScallopOption[Int] = opt[Int](descr = "Max number of rows per partition. This number will depend on available memory for executor.", noshort = true, default = Some(1))
  val csv: Map[String, String] = propsLong[String](name = "csv", descr = "Options to control csv read/write.  Some values may need to be escaped on the command line, such as delimiter='\\t'")(new CommaAllowedConverter)
  val output: ScallopOption[String] = opt[String](descr = "The HDFS path to the output directory", required = true, noshort = true)
  val outputFormat: ScallopOption[DataFormat] = opt[DataFormat](descr = "Specify the Output File Format", noshort = true, default = Some(CSV))(singleArgConverter((arg: String) => DataFormat.withName(arg.toUpperCase)))
  val overwrite: ScallopOption[Boolean] = opt[Boolean](descr = "Overwrite existing output", noshort = true)
  val parquet: Map[String, String] = propsLong[String](name = "parquet", descr = "Options to control parquet read/write")(new CommaAllowedConverter)
  val options: Map[String, String] = propsLong[String](name = "options", descr = "Options to add extra attributes to the result of join")(new CommaAllowedConverter)

  /**
   * processes File values for escaped characters
   */
  private def getProcessedOptions(options: Map[String, String]): Map[String, String] = {
    options.map { case (k, v) => (k, StringEscapeUtils.unescapeJava(v)) }
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


/*
 * Copyright 2017, 2021 Precisely. All rights reserved.
 * This document contains unpublished, confidential, and proprietary information of Precisely.
 * No disclosure or use of any portion of the contents of this document may be made without the express written consent of Precisely.
 */

package com.precisely.bigdata.addressing.spark.app

import com.precisely.bigdata.addressing.spark.app.DataFormat.DataFormat
import org.apache.commons.text.StringEscapeUtils
import org.rogach.scallop.exceptions.{Help, Version}
import org.rogach.scallop.{ArgType, ScallopConf, ScallopOption, ValueConverter, singleArgConverter}

class BaseCommandLine(arguments: Seq[String], driverClass: String, inputFieldsHelpAddition: String) extends ScallopConf(arguments) {
  version("spark-submit --class " + driverClass + " --master yarn --deploy-mode cluster [SPARK_SUBMIT_OPTIONS] <JAR_PATH> <JOB_OPTIONS>")

  val input: ScallopOption[String] = opt[String](descr = "The HDFS path to the input directory", required = true, noshort = true)
  val inputFormat: ScallopOption[DataFormat] = opt[DataFormat](descr = "Specify the Input File Format", noshort = true, default = Some(DataFormat.CSV))(singleArgConverter((arg: String) => DataFormat.withName(arg.toUpperCase)))
  val output: ScallopOption[String] = opt[String](descr = "The HDFS path to the output directory", required = true, noshort = true)
  val outputFormat: ScallopOption[DataFormat] = opt[DataFormat](descr = "Specify the Output File Format", noshort = true, default = inputFormat.toOption)(singleArgConverter((arg: String) => DataFormat.withName(arg.toUpperCase)))
  private val csv: Map[String, String] = propsLong[String](name = "csv", descr = "Options to control csv read/write.  Some values may need to be escaped on the command line, such as delimiter='\\t'")(new CommaAllowedConverter)
  private val parquet: Map[String, String] = propsLong[String](name = "parquet", descr = "Options to control parquet read/write ")(new CommaAllowedConverter)
  val limit: ScallopOption[Int] = opt[Int](descr = "limit output records", noshort = true)
  val overwrite: ScallopOption[Boolean] = opt[Boolean](descr = "Overwrite existing output", noshort = true)
  val combine: ScallopOption[Boolean] = opt[Boolean](descr = "Combine all output files into a single file.", noshort = true)
  val numPartitions: ScallopOption[Int] = opt[Int](descr = "Number of partitions to use for processing input", noshort = true)

  val resourcesLocation: ScallopOption[String] = opt[String](descr = "The resources location for Geocoding Library", required = true, noshort = true)
  val dataLocation: ScallopOption[List[String]] = opt[List[String]](descr = "The paths to the addressing datasets to use", noshort = true)
  val extractionLocation: ScallopOption[String] = opt[String](descr = "The path the addressing datasets will extract to. By default the dataset is extracted alongside the SPD.", noshort = true)
  val downloadLocation: ScallopOption[String] = opt[String](descr = "Location of the local directory where reference data will be downloaded to", noshort = true)
  val downloadGroup: ScallopOption[String] = opt[String](descr = "Group to grant permissions for download location", noshort = true)

  val inputFields: Map[String, String] = propsLong[String](descr = "List of input fields mapped to request fields. " + inputFieldsHelpAddition, name = "input-fields")(new CommaAllowedConverter)
  val preferencesFilepath: ScallopOption[String] = opt[String](descr = "Path of the addressing preferences file", noshort = true)
  val outputFields: ScallopOption[List[String]] = opt[List[String]](descr = "Fields from the addressing candidate to include in the output", required = true, noshort = true)
  val errorField: ScallopOption[String] = opt[String](descr = "Output field name for any error information during processing of individual input record", noshort = true)
  val jsonOutputField: ScallopOption[String] = opt[String](descr = "Output field name for Json Response during processing of individual input record", noshort = true)
  val country: ScallopOption[String] = opt[String](descr = "Country to use for all records when input country is not specified or is empty", noshort = true)
  val operation: ScallopOption[String] = choice(descr = "Operation to be performed, ", required = true, noshort = true, choices = Seq[String]("geocode", "verify", "lookup", "reverseGeocode"))


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
      case _ => throw new IllegalArgumentException("Unsupported input format")
    }
  }

  def getOutputOptions: Map[String, String] = {
    outputFormat.toOption match {
      case Some(DataFormat.CSV) => getProcessedOptions(csv)
      case Some(DataFormat.PARQUET) => getProcessedOptions(parquet)
      case _ => throw new IllegalArgumentException("Unsupported output format")
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

  override def verify(): Unit = {
    super.verify()

    if (inputFields.isEmpty) {
      throw new IllegalArgumentException("Required option 'input-fields' not found")
    }
  }

  // make sure, if someone has passed 'download-group' then 'download-location' should also be passed
  dependsOnAny(downloadGroup, List(downloadLocation))

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



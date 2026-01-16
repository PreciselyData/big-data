/*
 * Copyright 2017, 2020 Precisely. All rights reserved.
 * This document contains unpublished, confidential, and proprietary information of Precisely.
 * No disclosure or use of any portion of the contents of this document may be made without the express written consent of Precisely.
 */

package com.precisely.bigdata.addressing.spark.app

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes.DoubleType
import org.apache.spark.sql.{Column, DataFrame}

import scala.util.{Failure, Try}

object DriverUtils {
  private val COUNTRY_KEY = "country"
  private val LOOKUP_KEY_TYPE = "keyType"
  private val LOOKUP_KEY = "key"
  private val X = "x"
  private val Y = "y"
  val GEOCODE = "geocode"
  val VERIFY = "verify"
  val LOOKUP = "lookup"
  val REVERSE_GEOCODE = "reversegeocode"
  val REVERSE = "reverse"

  /**
   * Returns a Map of input addressing fields as a Column
   */
  val buildInputAddressingFields: (BaseCommandLine, DataFrame) => Column = (commandLine: BaseCommandLine, df: DataFrame) => {
    val addressFields = commandLine.inputFields.filterNot(_._1 == COUNTRY_KEY)
      .map(inputField =>
        (lit(inputField._1), concat_ws(" ", inputField._2.split(",").toSeq.map(field =>
          col(df.columns(getRequiredColumnIndex(inputField._1, Option(field), df)))
        ): _*))
      )
      .flatMap { case (k, v) => Seq(k, v) }
      .toList

    val countryCol: Column = buildCountryColumnField(commandLine, df)
    val toIndexedSeq: Seq[Column] = (addressFields :+ lit(COUNTRY_KEY) :+ countryCol).toIndexedSeq

    map(toIndexedSeq: _*)
  }

  /**
   * Returns a tuple of input lookup fields as Columns
   */
  val buildInputLookupFields: (BaseCommandLine, DataFrame) => (Column, Column, Column) = (commandLine: BaseCommandLine, df: DataFrame) => {
    val keyTypeColumn: Column = coalesce(
      commandLine.inputFields.get(LOOKUP_KEY_TYPE)
        .map(_ => col(df.columns(getRequiredColumnIndex(LOOKUP_KEY_TYPE, commandLine.inputFields.get(LOOKUP_KEY_TYPE), df))))
        .map(rawColumn => when(trim(rawColumn).eqNullSafe(""), lit(null)).otherwise(rawColumn))
        .getOrElse(lit(null)),
      lit(commandLine.lookupKeyType.getOrElse(null))
    )

    val keyColumnMap: Map[String, Column] = commandLine.inputFields.filterNot(_._1 == COUNTRY_KEY)
      .map(inputField => (inputField._1, col(df.columns(getRequiredColumnIndex(inputField._1, Option(inputField._2), df)))))

    val keyColumn: Column = keyColumnMap.getOrElse(LOOKUP_KEY,
      throw new IllegalArgumentException(s"Lookup key column '$LOOKUP_KEY' not provided in the input fields.")
    )

    val countryCol: Column = buildCountryColumnField(commandLine, df)

    (keyTypeColumn, keyColumn, countryCol)
  }

  /**
   * Returns a tuple of input reverse geocode fields as Columns
   */
  val buildInputReverseGeocodeFields: (BaseCommandLine, DataFrame) => (Column, Column, Column) = (commandLine: BaseCommandLine, df: DataFrame) => {
    val ordinatesMap: Map[String, Column] = commandLine.inputFields.filterNot(_._1 == COUNTRY_KEY)
      .map(inputField => (inputField._1, col(df.columns(getRequiredColumnIndex(inputField._1, Option(inputField._2), df)))))

    val xCol: Column = ordinatesMap.getOrElse(X,
      throw new IllegalArgumentException(s"Reverse Geocode key column '$X' not provided in the input fields.")
    ).cast(DoubleType)

    val yCol: Column = ordinatesMap.getOrElse(Y,
      throw new IllegalArgumentException(s"Reverse Geocode key column '$Y' not provided in the input fields.")
    ).cast(DoubleType)

    val countryCol: Column = buildCountryColumnField(commandLine, df)

    (xCol, yCol, countryCol)
  }

  /**
   * Returns a column index from the input fields (either based on a column name or simple literal column index), and if not present throws an exception.
   */
  private def getRequiredColumnIndex(inputFieldKey: String, inputFieldDefinition: Option[String], df: DataFrame): Int = {
    inputFieldDefinition.map(col =>
      Try[Int](df.schema.fieldIndex(col)).recoverWith {
        case _: IllegalArgumentException =>
          Try(Integer.parseInt(col)).recoverWith {
            case _ => Failure(new IllegalArgumentException("Column does not exist: " + col))
          }
      }.get
    ) match {
      case Some(result) => result
      case None => throw new IllegalArgumentException("Required input field not provided: " + inputFieldKey)
    }
  }

  private def buildCountryColumnField(commandLine: BaseCommandLine, df: DataFrame) = {
    coalesce(
      commandLine.inputFields.get(COUNTRY_KEY)
        .map(_ => col(df.columns(getRequiredColumnIndex(COUNTRY_KEY, commandLine.inputFields.get(COUNTRY_KEY), df))))
        .map(rawColumn => when(trim(rawColumn).eqNullSafe(""), lit(null)).otherwise(rawColumn))
        .getOrElse(lit(null)),
      lit(commandLine.country.getOrElse(null))
    )
  }
}

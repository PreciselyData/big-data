/*
 * Copyright 2017, 2020 Precisely. All rights reserved.
 * This document contains unpublished, confidential, and proprietary information of Precisely.
 * No disclosure or use of any portion of the contents of this document may be made without the express written consent of Precisely.
 */

package com.precisely.bigdata.addressing.spark.app

import org.apache.spark.sql.functions.{col, lit, trim, when}
import org.apache.spark.sql.{Column, DataFrame, functions}
import org.rogach.scallop.ScallopOption

import scala.util.{Failure, Try}

object DriverUtils {
  /**
   * Returns a column index from the input fields (either based on a column name or simple literal column index), and if not present throws an exception.
   */
  def getRequiredColumnIndex(inputFieldKey: String, inputFieldDefinition: Option[String], df: DataFrame): Int = {
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

  /**
   * Creates a column definition that uses logic to fallback from null value in a specified column to a literal default value.
   */
  def buildFallbackToLiteral(inputField: String, inputLiteral: ScallopOption[String], df: DataFrame, commandLine: BaseCommandLine): Column = {
    functions.coalesce(
      commandLine.inputFields.get(inputField)
        .map(_ => col(df.columns(getRequiredColumnIndex(inputField, commandLine.inputFields.get(inputField), df))))
        .map(rawColumn => when(trim(rawColumn).eqNullSafe(""), lit(null)).otherwise(rawColumn))
        .getOrElse(lit(null)),
      lit(inputLiteral.getOrElse(null))
    )
  }
}

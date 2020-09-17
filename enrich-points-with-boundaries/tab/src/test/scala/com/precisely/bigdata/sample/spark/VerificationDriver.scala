/*
 * Copyright 2019,2020 Precisely
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.precisely.bigdata.sample.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object VerificationDriver {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setIfMissing("spark.master", "local[*]")
    sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    val session = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    val resultDataPath = args(0)

    val resultDF = session.read.parquet(resultDataPath)

    //spot check some results
    import org.scalatest.Assertions._
    assert(resultDF.count == 548307)

    val sampleRecord = resultDF.filter(resultDF("PBKEY") === "P00002T1QGHL").first()
    assert(sampleRecord.getString(resultDF.schema.fieldIndex("auto_theft_category")) === "High")
  }
}

/*
 * Copyright 2019,2021 Precisely
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

import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.util.logging.{ConsoleHandler, Level, Logger}

@RunWith(classOf[JUnitRunner])
class MultipassAddressingTester extends AnyFunSuite with BeforeAndAfterAll {

  private val parquetLogger = Logger.getLogger("org.apache.parquet") //holding on to logger to persist log level changes

  override def beforeAll() {
    //turn down the logging for spark logs which uses log4j
    org.apache.log4j.Logger.getRootLogger.setLevel(org.apache.log4j.Level.ERROR) //change log level if more debug information is needed
    org.apache.log4j.Logger.getRootLogger.addAppender(new org.apache.log4j.ConsoleAppender())

    //turn down the logging for the parquet package which uses java.util.logging
    val handler = new ConsoleHandler
    handler.setLevel(Level.SEVERE)
    parquetLogger.addHandler(handler)
  }

  test("Execute sample and assert output") {
    val paths = new Array[String](5)

    paths(0) = "./resources"
    paths(1) = "./data/referenceData"
    paths(2) = "./build/download"
    paths(3) = "./data/inputData"
    paths(4) = "./build/output"

    // Execute our Multipass Geocoding sample
    MultipassAddressing.main(paths)

    //verify the results of the sample run
    VerificationDriver.main(Array(paths(4)))
  }
}

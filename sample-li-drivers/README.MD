![Precisely](../Precisely_Logo.png)

# Location Intelligence Driver Sample

This sample for the Spark Location Intelligence SDK in Scala demonstrates how to create a custom driver that will be helpful in
running spark-submit based implementations for Location Intelligence.
Although, there is already default driver classes for each spatial operation available in the SDK jar, this sample project will help you understand
how to create your own driver class and customize it as per your requirements.

## Prerequisites

* JDK 17 or above
* Cluster having Apache Spark 3.x or above

## Building the Sample Project

1. Download the Location Intelligence SDK for Big Data distribution zip and extract the contents.
2. Place the _location-intelligence-bigdata-li-sdk-spark&lt;scala-version&gt;&lt;sdk-version&gt;.jar_ into the `/libs`
   directory of this sample, according to your scala version, and uncomment the corresponding scala version libraries in
   `/build.gradle`.
3. Driver classes
   * `SampleHexGenLIDriver.scala`
   * `SampleJoinByDistanceLIDriver.scala`
   * `SamplePointInPolygonLIDriver.scala`
   * `SampleSearchNearestLIDriver.scala`

   are available in this project. You can customize these classes as per your requirements.
4. Use the following command from the root of the sample to build:
    ```
    ./gradlew build
    ``` 

## Running your sample driver on a cluster

To execute the sample driver on a cluster, follow the following steps:

1. A jar will be created after building your customized code.
2. Copy the sample jar to a Hadoop cluster. (For EMR or Databrics, add this jar as a library dependency)
3. You can then use the `spark-submit` command to execute the driver on the cluster.

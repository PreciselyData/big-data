![Precisely](../Precisely_Logo.png)

# Geo Addressing Driver Sample

This sample for the Spark Addressing SDK in Scala demonstrates how to create a custom driver that will be helpful in
running spark-submit based implementations for Geo Addressing.
Although, there is already a default driver class available in the SDK jar, this sample project will help you understand
how to create your own driver class and customize it as per your requirements.

## Prerequisites

* JDK 17 or above
* Cluster having Apache Spark 3.x or above

## Building the Sample Project

1. Download the Geo Addressing SDK for Big Data distribution zip and extract the contents.
2. Place the _geo-addressing-bigdata-addressing-sdk-spark&lt;scala-version&gt;&lt;sdk-version&gt;.jar_ into the `/libs`
   directory of this sample, according to your scala version, and uncomment the corresponding scala version libraries in
   `/build.gradle`.
3. The SampleAddressingDriver.scala file contains the main driver class.
   You can customize this class as per your requirements.
4. Use the following command from the root of the sample to build:
    ```
    ./gradlew build
    ``` 

## Running your sample driver on a cluster

To execute the sample driver on a cluster, follow the following steps:

1. A jar will be created after building your customized code.
2. Copy the sample jar to a Hadoop cluster. (For EMR or Databrics, add this jar as a library dependency)
3. You can then use the following command to execute the driver on the cluster:
    ```sh
    spark-submit --class com.precisely.bigdata.sample.spark.SampleAddressingDriver \
    --master yarn \
    --deploy-mode cluster \
    <sample-jar-name>.jar \
    --operation geocode \
    --resources-location <resources-location> \
    --data-location <data-location> \ 
    --download-location <download-location e.g. ./downloads> \
    --preferences-filepath <path-to-preferences-file> \
    --input <input-file-path> \
    --input-format=<csv or parquet> \
    --csv <e.g. header=true> \
    --output <output-file-path> \
    --output-format=<csv or parquet> \
    --parquet <e.g. compression=gzip> \
    --input-fields <e.g. addressLines[0]=0 addressLines[1]=1 admin1=2> \
    --output-fields <e.g. address.formattedStreetAddress address.formattedLocationAddress location.feature.geometry.coordinates.x location.feature.geometry.coordinates.y> \
    --combine \
    --limit 20 \
    <any-other-customized-arguments>
     ```
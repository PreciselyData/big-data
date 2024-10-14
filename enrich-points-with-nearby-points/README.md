![Precisely](../Precisely_Logo.png)

# Point Enrichment Sample
This sample demonstrates how to use Spark to enrich a CSV containing point data with attributes associated with 
points within some max distance. Enriching your point data with another dataset can provide you with 
additional context. For example, let's say that you have a list of customers represented by the Address Fabric
sample below. Understanding which businesses are nearby to each of them can help determine where they might be
shopping. This sample demonstrates joining the Address Fabric to the Point of Interest (POI) dataset with a distance
of 0.5 miles.

## Data
This sample includes the following data located in the `/data` folder:

* **Address Fabric**:
Sample of points in San Francisco from Precisely 
[Address Fabric Data](https://dataguide.precisely.com/address-fabric-data).

* **Points of Interest**:
Sample of points in San Francisco from Precisely [Points of Interest Data](https://dataguide.precisely.com/).

By downloading the sample data, you are agreeing to a 30-day trial and our [Evaluation Terms](https://www.precisely.com/legal/licensing/trial-evaluation-agreement-terms-u-s?utm_medium=Redirect-PB&utm_source=Direct-Traffic).

## Building the sample
1. Download the Spectrum Location Intelligence for Big Data distribution and extract the contents.
1. Place the _spectrum-bigdata-li-sdk-spark2-&lt;version&gt;.jar_ into the `/lib` directory of this sample.
1. Use the following command from the root of the sample to build:
    ```
    gradlew build
    ```
    This command will compile the sample code, and will also execute the sample code via a JUnit test that is also included.  The 
    test will run the sample locally using SparkSession and then do some simple JUnit asserts to verify the sample
    executed successfully.  To only build the sample code without executing the test, you can exclude the test using the following command:
    ```
    gradlew build -x test
    ```

## Running the sample on a cluster
To execute the sample on a cluster, complete the following steps on your Hadoop cluster:
1. Copy the input data to a Hadoop cluster. This data is available in data directory of the sample.
1. Copy the input data to HDFS:
    ```
    hadoop fs -copyFromLocal <localSampleDirectory>/enrich-points-with-nearby-points/data/WPPOI_USA.txt <hdfsDirectory>/data
    hadoop fs -copyFromLocal <localSampleDirectory>/enrich-points-with-nearby-points/data/us_address_fabric_san_francisco.txt <hdfsDirectory>/data
    ```
1. Create an output directory on HDFS:
    ```
    hadoop fs -mkdir <hdfsDirectory>/output
    ```
1. Copy over all library dependencies to a location on the cluster (&lt;localDirectory&gt;/lib)
   1. _spectrum-bigdata-li-sdk-spark2-&lt;version&gt;.jar_
   1. _enrich-points-with-nearby-points-1.0.0.jar_
1. Execute a spark job on the cluster:
   ```
   spark-submit --class com.precisely.bigdata.sample.spark.PointEnrichment --master yarn --deploy-mode cluster --jars <localDirectory>/lib/spectrum-bigdata-li-sdk-spark2-<version>.jar <localDirectory>/lib/enrich-points-with-nearby-points-1.0.0.jar hdfs:///<hdfsDirectory>/data/WPPOI_USA.txt hdfs:///<hdfsDirectory>/data/us_address_fabric_san_francisco.txt hdfs:///<hdfsDirectory>/output
   ```

## Hadoop libraries
Included with the sample are the Hadoop libraries required for running the spark sample on Windows.  The libraries were 
acquired through https://github.com/steveloughran/winutils/tree/master/hadoop-2.7.1.  These libraries are referenced in 
the build.gradle file by adding the path to them as an environment variable to the included unit test.  These libraries 
are included in the sample in accordance to this license: https://github.com/steveloughran/winutils/blob/master/LICENSE

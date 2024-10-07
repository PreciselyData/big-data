![Precisely](../../Precisely_Logo.png)

Enrichment Sample
---------------------
This sample demonstrates how to use Spark to enrich a CSV containing point data with attributes from a spatial file based on 
a point in polygon search. Enriching your point data with another dataset can provide you with 
additional context. For example, let's say that you have a list of clients represented by the Address Fabric
sample below. Understanding what risks are associated with those clients can help you price products such as insurance. 
This sample demonstrates joining the Address Fabric to the Crime Index data.

## Data
This sample includes the following data located in the `/data` folder:

* **Address Fabric**:
Sample of points near San Francisco from Precisely 
[Address Fabric Data](https://dataguide.precisely.com/address-fabric-data).

* **Crime Data**:
Sample crime data from Precisely 
[Crime Data](https://dataguide.precisely.com/).

By downloading the sample data, you are agreeing to a 30-day trial and our 
[Evaluation Terms](https://www.precisely.com/legal/licensing/trial-evaluation-agreement-terms-u-s?utm_medium=Redirect-PB&utm_source=Direct-Traffic).

## Projects
Multiple subprojects show examples using different data formats.
   * **shape**: Uses shape data under `.\data\CrimeIndex_Sample\shape\ `
   * **tab**: Uses tab data under `.\data\CrimeIndex_Sample\tab\ `
   * **geodatabase**: Uses geodatabase at `.\data\CrimeIndex_Sample\CrimeIndex.gdb ` and table name is `CrimeIndex_2019_Sample`

A fourth subproject shows an example where a schema is explicitly declared when using spatial filters.
   * **using-schema**: uses the tab sample but instead declares the schema of the search response

## Building the sample
Each sample includes a gradle build system around it. Choose which subproject to run from the 4 listed above. 
The following documentation will use the **tab** subproject. 

1. Download the Spectrum Location Intelligence for Big Data distribution and extract the contents.
1. Place the _spectrum-bigdata-li-sdk-spark2-&lt;version&gt;.jar_ into the `/lib` directory of this sample.
1. Use the following command from the root of the sample to build:
    ```
    gradlew :tab:build
    ```
    This command will compile the sample code, execute the sample code via a JUnit test that is also included, and generate 
    a single fat jar that contains all the required code.  The test will run the sample locally using SparkSession and then 
    do some simple JUnit asserts to verify the sample executed successfully.  To only build the sample code without 
    executing the test, you can exclude the test using the following command:
    ```
    gradlew :tab:build -x test
    ```

## Running the sample on a cluster
To execute the sample on a cluster, complete the following steps on your Hadoop cluster:
1. Copy the input data to a Hadoop cluster. This data is available in the data directory of the sample.
1. Copy the input data to HDFS:
     ```
     hadoop fs -copyFromLocal <localSampleDirectory>/enrich-points-with-boundaries/data/us_address_fabric_san_francisco.txt <hdfsDirectory>/data/input
     hadoop fs -copyFromLocal <localSampleDirectory>/enrich-points-with-boundaries/data/CrimeIndex_Sample/tab <hdfsDirectory>/data/
     ```
1. Copy over all library dependencies to a location on the cluster (&lt;localDirectory&gt;/lib)
   1. _spectrum-bigdata-li-sdk-spark2-&lt;version&gt;.jar_
   1. _/enrich-points-with-boundaries/tab/build/libs/enrich-points-with-boundaries-tab-1.0.0.jar_
1. Create a local directory on all nodes in the cluster (&lt;localDirectory&gt;/downloads) This 
directory is used to locally cache the TAB data for use during the search.
1. Execute a spark job on the cluster:
   ```
   spark-submit --class com.precisely.bigdata.sample.spark.TABEnrichment --master yarn --deploy-mode cluster --jars <localDirectory>/lib/spectrum-bigdata-li-sdk-spark2-<version>.jar <localDirectory>/lib/enrich-points-with-boundaries-tab-1.0.0.jar hdfs:///<hdfsDirectory>/data/CrimeIndex_Sample/tab hdfs:///<hdfsDirectory>/data/us_address_fabric_san_francisco.txt hdfs:///<hdfsDirectory>/output <localDirectory>/downloads
    ```

## Hadoop libraries
Included with the sample are the Hadoop libraries required for running the spark sample on Windows.  The libraries were 
acquired through https://github.com/steveloughran/winutils/tree/master/hadoop-2.7.1.  These libraries are referenced in 
the build.gradle file by adding the path to them as an environment variable to the included unit test.  These libraries 
are included in the sample in accordance to this license: https://github.com/steveloughran/winutils/blob/master/LICENSE


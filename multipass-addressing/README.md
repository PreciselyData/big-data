![Precisely](../Precisely_Logo.png)

# Multipass Addressing Sample
This sample for the Spark Verify and Geocoding API in Scala demonstrates how to improve geocoding results by performing verify first and then geocoding. With this multipass-addressing, for all first-pass results without point-level precision, a second geocoding pass is run using single line address matching, which may return more accurate geocodes.

## Data
This sample includes the following data located in the `/data` folder:

* **Addresses**: Sample list of Washington, DC addresses.
* **Geocoding Reference Data**: Sample geocoding dataset for Washington, DC.

## Building the sample
1. Download the Spectrum Addressing for Big Data distribution and extract the contents.
1. Place the _spectrum-bigdata-addressing-sdk-spark2-&lt;version&gt;.jar_ into the `/lib` directory of this sample.
1. Place the contents of the `/resources` directory into the `/resources` directory of this sample.
1. Place the `/sampleData/DC/sampleDictionary` directory into the `/data/referenceData` directory of this sample.
1. Use the following command from the root of the sample to build:
    ```
    gradlew build
    ```
   This command will compile the sample code, execute the sample code via a JUnit test that is also included, and generate
   a jar that contains all the compiled sample code.  The test will run the sample locally using SparkSession, and then
   do some simple JUnit asserts to verify the sample executed successfully.  To only build the sample code without
   executing the test, you can exclude the test using the following command:
    ```
    gradlew build -x test
    ```

## Running the sample on a cluster
To execute the sample on a cluster, complete the following steps on your Hadoop cluster:
1. Copy the sample to a Hadoop cluster.
1. Copy the dataset, resources, and input data to HDFS:
     ```
     hadoop fs -copyFromLocal <localSampleDirectory>/multipass-addressing/data/inputData/addresses.csv <hdfsDirectory>/data/input
     hadoop fs -copyFromLocal <localSampleDirectory>/multipass-addressing/data/referenceData <hdfsDirectory>/data/referenceData
     hadoop fs -copyFromLocal <localSampleDirectory>/multipass-addressing/resources <hdfsDirectory>/resources
     ```
1. Copy over all library dependencies to a location on the cluster (&lt;localDirectory&gt;/lib)
    1. _spectrum-bigdata-addressing-sdk-spark2-&lt;version&gt;.jar_
    1. _/multipass-addressing/build/libs/multipass-addressing-&lt;version&gt;.jar_
1. Create a local directory on all nodes in the cluster (&lt;localDirectory&gt;/downloads) This
   directory is used to locally download and cache the geocoding datasets.
1. Execute a spark job on the cluster.
   ```sh
   spark-submit --class com.precisely.bigdata.sample.spark.MultipassAddressing --master yarn --deploy-mode cluster --jars <localDirectory>/lib/spectrum-bigdata-addressing-sdk-spark2-<version>.jar <localDirectory>/lib/multipass-addressing-<version>.jar hdfs:///<hdfsDirectory>/input/addresses.csv hdfs:///<hdfsDirectory>/resources hdfs:///<hdfsDirectory>/data/referenceData <localDirectory>/downloads hdfs:///<hdfsDirectory>/output
    ```

## Hadoop libraries
Included with the sample are the Hadoop libraries required for running the spark sample on Windows.  The libraries were
acquired through https://github.com/steveloughran/winutils/tree/master/hadoop-2.7.1.  These libraries are referenced in
the build.gradle file by adding the path to them as an environment variable to the included unit test.  These libraries
are included in the sample in accordance to this license: https://github.com/steveloughran/winutils/blob/master/LICENSE

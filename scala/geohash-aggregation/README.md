![Precisely](../../Precisely_Logo.png)

# Geohash Aggregation Sample
This sample demonstrates how to return geohashes for point locations and aggregate the data in each geohash. 
The locations in this sample come from social service requests into the NYC 311 information system.
Each request contains a NYC agency responsible for handling the request. 
The sample computes a geohash value for each location at a geohash precision of 7. 
The data is then grouped by a common geohash and counted.

## Data
This sample includes the following data located in the `/data` folder:

* **311 Data**: Represents a subset of social service requests into the NYC 311 information system.
These files are from the [NYC Open Data website](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9). 
Each entry contains a NYC agency and the longitude and latitude coordinates that represent the location of the request. 
From its original form on the website, our sample has been filtered to include approximately 7 million incidents from five agencies:
  * **DOT**: Department of Transportation
  * **HPD**: Department of Housing Preservation and Development
  * **NYPD**: New York City Police Department
  * **DEP**: Department of Environmental Protection
  * **DSNY**: New York City Department of Sanitation
  
   | Agency | Longitude  | Latitude  |
   | :----- | ---------: | --------: |
   |  DSNY  | -73.922369 | 40.670104 |
   |  DSNY  | -73.922369 | 40.670104 |
   |  NYPD  | -73.957987 | 40.713169 |

## Building the sample
1. Download the Spectrum Location Intelligence for Big Data distribution and extract the contents.
1. Place the _spectrum-bigdata-li-sdk-spark2-&lt;version&gt;.jar_ into the `/lib` directory of this sample.
1. Use the following command from the root of the sample to build:
    ```
    gradlew build
    ```
    This command will compile the sample code, and will also execute the sample code via a JUnit test that is also included.
    The test will run the sample locally using SparkSession and then do some simple JUnit asserts to verify the sample executed successfully. 
    To only build the sample code without executing the test, you can exclude the test using the following command:
    ```
    gradlew build -x test
    ```

## Running the sample on a cluster
To execute the sample on a cluster, complete the following steps on your Hadoop cluster:
1. Copy the input data to a Hadoop cluster. This data is available in data directory of the sample.
1. Copy the input data to HDFS:
    ```
    hadoop fs -copyFromLocal <localSampleDirectory>/geohash-aggregation/data/311data.csv <hdfsDirectory>/data
    ```
1. Create an output directory on HDFS:
    ```
    hadoop fs -mkdir <hdfsDirectory>/output
    ```    
1. Copy over all library dependencies to a location on the cluster (&lt;localDirectory&gt;/lib)
   1. _spectrum-bigdata-li-sdk-spark2-&lt;version&gt;.jar_
   1. _geohash-aggregation-1.0.0.jar_
1. Execute a spark job on the cluster:
   ```
   spark-submit --class com.precisely.bigdata.sample.spark.GeohashAggregation --master yarn --deploy-mode cluster --jars <localDirectory>/lib/spectrum-bigdata-li-sdk-spark2-<version>.jar <localDirectory>/lib/geohash-aggregation-1.0.0.jar hdfs:///<hdfsDirectory>/data/311data.csv hdfs:///<hdfsDirectory>/output
   ```

## Analyzing the output
The output will be a parquet table at the output location specified when running the spark-submit command. 
Use `spark.read.parquet("hdfs:///<hdfsDirectory>/output")` to do further analysis on the table.

| Geohash | DEP| DOT|DSNY| HPD|NYPD|Count|polygon          |
| :-----  |---:|---:|---:|---:|---:| ---:| :-------------- |
| dr5nrxu |2| 6|5| |1|14|POLYGON ((-74.196...|
| dr5nwgm | | 1| | | | 1|POLYGON ((-74.227...|
| dr5nx4k |6|31|4| |2|43|POLYGON ((-74.218...|


## Hadoop libraries
Included with the sample are the Hadoop libraries required for running the spark sample on Windows. 
The libraries were acquired through https://github.com/steveloughran/winutils/tree/master/hadoop-2.7.1. 
These libraries are referenced in the build.gradle file by adding the path to them as an environment variable to the included unit test. 
These libraries are included in the sample in accordance to this license: https://github.com/steveloughran/winutils/blob/master/LICENSE

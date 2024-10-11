![Precisely](Precisely_Logo.png "Precisely")

# Samples

A collection of samples demonstrating how to use Location Intelligence for Big Data. This sample supports till Spark 3.5.3.

### PySpark Addressing Sample
This sample demonstrates how to use PySpark for executing Geo Addressing SDK APIs. This sample contains setting up Addressing in Databricks Cluster using PySpark. Also, sample notebooks are provided to use Snowflake Spark Connector in Python for the input data in Snowflake for Databricks and EMR clusters. 

### Enrichment Sample using Boundaries
This sample demonstrates how to use Spark to enrich a CSV containing point data with attributes from a spatial file based on
a point in polygon search. Enriching your point data with another dataset can provide you with
additional context. For example, let's say that you have a list of clients represented by the Address Fabric
sample below. Understanding what risks are associated with those clients can help you price products such as insurance.
This sample demonstrates joining the Address Fabric to the Crime Index data.

### Point Enrichment Sample
This sample demonstrates how to use Spark to enrich a CSV containing point data with attributes associated with
points within some max distance. Enriching your point data with another dataset can provide you with
additional context. For example, let's say that you have a list of customers represented by the Address Fabric
sample below. Understanding which businesses are nearby to each of them can help determine where they might be
shopping. This sample demonstrates joining the Address Fabric to the Point of Interest (POI) dataset with a distance
of 0.5 miles.

### Geohash Aggregation Sample
This sample demonstrates how to return geohashes for point locations and aggregate the data in each geohash.
The locations in this sample come from social service requests into the NYC 311 information system.
Each request contains a NYC agency responsible for handling the request.
The sample computes a geohash value for each location at a geohash precision of 7.
The data is then grouped by a common geohash and counted.

### Multipass Geocoding Sample
This sample for the Spark Geocoding API in Scala demonstrates how to improve geocoding results by performing multipass geocoding. With multipass geocoding, for all first-pass results without point-level precision, a second geocoding pass is run using single line address matching, which may return more accurate geocodes.

### Databricks Geocoding Sample
This sample demonstrates setting up geocoding on a databricks cluster.

### Multipass Addressing Sample
This sample for the Spark Addressing SDK in Scala demonstrates how to improve geocoding results by performing verify first and then geocoding. With this multipass addressing example, for all results without address level precision, a second geocoding pass is run using single line input address, which may increase match rate.

### Spark UDF notebook for drivers and UDF(Python, Scala)
This sample demonstrates using location intelligence for big data to run spatial geometry operations using spark sql using scala or python notebooks. We can also run spatial operations like Point in Polygon, Search Nearest, Join by Distance and Hexgen as Spark drivers using Spark submit job. 

<ol>
  <li><strong>Pyspark notebooks for Spark UDF</strong> https://github.com/PreciselyData/big-data/blob/release-5.2.1/pyspark/databricks-li/LI-SDK-UDF-Pyspark.ipynb</li>
  <li><strong>Pyspark notebooks for Spark drivers</strong> https://github.com/PreciselyData/big-data/blob/release-5.2.1/pyspark/databricks-li/LI-SDK-Drivers-Pyspark.ipynb</li>
  <li><strong>Scala notebooks for Spark UDF</strong> https://github.com/PreciselyData/big-data/blob/release-5.2.1/scala/databricks-li/Spark-sql-functions.scala</li>
  <li><strong>Scala notebooks for Spark drivers</strong> https://github.com/PreciselyData/big-data/tree/release-5.2.1/scala/databricks-li</li>
</ol>






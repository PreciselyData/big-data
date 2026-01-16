![Precisely](../Precisely_Logo.png)

# Geo Addressing SDK for Big Data in Databricks
This sample for Databricks demonstrates how to install, configure, and execute an geo addressing capabilities of Geo Addressing SDK in Databricks.

> **NOTE: For Dtabricks Unity Catalogue Shared Compute, the Geo Addressing SDK for Big Data is not supported because of the architecture limitations on Shared Compute. We recommend using Personal or Single User Compute for running the Geo Addressing SDK for Big Data.**

## Getting Started

Below are the steps mentioned to get started with using Geo Addressing SDK for Big Data in Databricks.

## 1. Start the Compute/Cluster

Refer to the [version chart](#version-chart) at the end of this document 
to determine the appropriate java and scala versions for your Geo Addressing SDK for Big Data version.

Use `JNAME=zulu17-ca-amd64` to enable Java 17.

![alt text](jname-17.png)

## 2. Installing Reference Data

**NOTE**: We recommend to extract the reference data during runtime, as downloading at dbfs path or volumes will create performance issues.

- Collect your Data Experience API Key and Secret Key by visiting https://data.precisely.com/autodownload.  You will need these for the Addressing Installation notebook.

- Import the [Installation Guide Notebook](./Installing_SDK_and_Reference_Data.ipynb) in your Databricks account and follow the instructions by replacing the variables to install the reference data.
The Addressing Installation notebook contains the commands necessary to install the addressing libraries and reference data. The first command provides configuration variables for the notebook and will need to be updated with values specific to your environment. After updating the configuration section, you can execute each cell individually or run the entire notebook to perform the installation.


- You will also need to provide the URL for the location of the Geo Addressing SDK for Big Data product distribution. You should be able to find this URL in your product delivery email from Precisely. See these links for information on creating AWS presigned https://docs.aws.amazon.com/cli/latest/reference/s3/presign.html , and for Azure https://docs.microsoft.com/en-us/rest/api/storageservices/Service-SAS-Examples?redirectedfrom=MSDN . If you prefer, you may manually copy the product distribution to your dbfs and use a file:///dbfs/<path_on_dbfs> URL.

- Once those changes have been made, you can execute the cell.
The rest of the cells can be executed in order.


**NOTE**: This sample integrates Databricks with the Precisely Data Experience in order to access and configure your licensed reference data, which is required for running the geocoder.  Using your credentials you will be assured to be using the most recent data vintage.  This also expedites the setting up of data on your cluster.


## 3. Installing the Geo Addressing SDK Jar File in the Cluster

<strong> After executing the Installation Guide Notebook</strong>, you will see a jar called out in the results.  

That jar should be added as a library to your cluster https://docs.databricks.com/libraries/index.html.


**NOTE**: The sdk jar library for pyspark and scala notebooks to be attached is same.


## 4. Running the Geo Addressing Application


A sample notebook is provided along with required code snippts to run the Geo Addressing in Databricks for geocoding.

For PySpark, refer [Geo_Addressing_Demo Notebook](./pyspark/Geo_Addressing_Demo.ipynb) for executing Geo Addressing Application.

For Scala, refer [Geo_Addressing_Demo Notebook](./scala/Geo_Addressing_Demo.scala) for executing Geo Addressing Application.

# Useful Links and References

Refer the following links for more information about Geo Addressing SDK for Big Data and it's usage:

- [Product User Guide](https://help.precisely.com/r/p/Geo-Addressing-SDK-for-Big-Data/pub/5.2.2/en-US/Geo-Addressing-SDK-for-Big-Data-Guide)
- [Reference Documentation Landing Page](https://docs.precisely.com/docs/sftw/hadoop/landingpage/index.html)



# Version Chart

Refer to the following chart for GA-SDK version against Geo Addressing SDK for Big Data Version.

| Geo Addressing SDK for Big Data Version | GA-SDK Version | JDK Version | Scala Version |
|-----------------------------------------|----------------|-------------|---------------|
| 5.1.0.8 - 5.1.0.10                      | 5.1.27         | 8           | 2.12          |
| 5.1.0.11                                | 5.1.682        | 8           | 2.12          |
| 5.2.0.0                                 | 5.1.796        | 8           | 2.12          |
| 5.2.1                                   | 5.1.854        | 11          | 2.12          |
| 5.2.2                                   | 11.1.1250      | 11          | 2.12          |
| 11.2.0                                  | 11.2.228       | 17          | 2.12, 2.13    |
| 11.2.1                                  | 11.2.463       | 17          | 2.12, 2.13    |

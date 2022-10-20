![Precisely](../../Precisely_Logo.png)

# Geo Addressing SDK Sample using Snowflake Connector for Spark
This samples demonstrates how to configure, and execute an addressing process of Geo Addressing SDK for the data in Snowflake using Snowflake Connector for Spark in Databricks and EMR clusters.

For more information, please follow the official documentation of Snowflake Connector for Spark: https://docs.snowflake.com/en/user-guide/spark-connector.html


# Pre-Requisites:
This sample project has notebooks only related to connecting to Snowflake Table, Reading the data in a DataFrame and Executing the Addressing process. 

For the installation of Geo-Addressing SDK in EMR, visit https://docs.precisely.com/docs/sftw/hadoop/landingpage/docs/geocoding/webhelp/Geocoding/source/geocoding/addressing/addressing_sdk_title.html. 

For the installation of Geo-Addressing SDK in Databricks, follow [guidelines](../databricks-addressing/README.md)

## Snowflake Connector for Spark in Databricks
After installation of Geo-Addressing SDK and configuring the data in Databricks using [the guidelines provided here](../databricks-addressing/README.md), Snowflake Connector in Databricks makes use of Secrets (https://docs.databricks.com/security/secrets/index.html). Please create the secrets containing the Username and Password of the Snowflake User Account.

A sample code for using Geo-Addressing SDK for Snowflake Connector in Databricks is provided [here](../snowflake-connector/databricks/Snowflake_Connector_Addressing_Demo.ipynb)

For more information, please visit the official Snowflake Documentation: https://docs.snowflake.com/en/user-guide/spark-connector-databricks.html


## Snowflake Connector for Spark in EMR
After installation of Geo-Addressing SDK and data in EMR from https://docs.precisely.com/docs/sftw/hadoop/landingpage/docs/geocoding/webhelp/Geocoding/source/geocoding/addressing/addressing_sdk_title.html, the following jars need to be added in the Spark Context before using Snowflake Connector for Spark:
1. Snowflake JDBC Connector (https://search.maven.org/search?q=snowflake-jdbc)
2. Snowflake Connector for Spark (https://search.maven.org/search?q=a:spark-snowflake_2.12)

The sample code for using Geo-Addressing SDK for Snowflake Connector in EMR is provided [here](../snowflake-connector/emr/EMR_PySpark_Addressing_Snowflake_Connector.ipynb)

For more information, please visit the official Snowflake Documentation: https://docs.snowflake.com/en/user-guide/spark-connector-install.html
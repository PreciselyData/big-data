![Precisely](../Precisely_Logo.png)

# Geo Addressing SDK for Big Data Sample for Databricks

This sample for Databricks demonstrates how to install, configure, and execute a geocoding process of Geo
Addressing SDK For Big Data in Databricks.
After cloning or downloading the repo, you will have 2 scala files to be imported into your
workspace https://docs.databricks.com/notebooks/notebooks-manage.html. You will also have a jar,
/lib/precisely-bigdata-pdx-sdk2.8-full.jar which will be added to your dbfs via a cell in the installation notebook.

## Data

This sample integrates Databricks with the Precisely Data Experience in order to access and configure your licensed
reference data, which is required for running the geocoder. Using your credentials you will be assured to be using the
most recent data vintage. This also expedites the setting up of data on your cluster.

* **Geocoding Reference Data**: ex: USA-MasterLocationData, USA-TomTom.

## Setting up DataExperience integration

1) Collect your Data Experience API Key and Secret Key by visiting https://data.precisely.com/autodownload. You will
   need these for the Geocoding Installation notebook.
2) The DataExperience SDK has been prebuilt and included in the repo at /lib/precisely-bigdata-pdx-sdk2.8-full.jar. It
   will be added to your dbfs via a cell in the installation notebook.

## Installing Geo Addressing

The Geocoding Installation notebook contains the commands necessary to install the geocoding libraries and reference
data. The first command provides configuration variables for the notebook and will need to be updated with values
specific to your environment. After updating the configuration section, you can execute each cell individually or run
the entire notebook to perform the installation.

The Configuration and Variables cell builds a shell script to hold values needed by the other cells. This will be the
main cell that will require your edits.
<ol>

  <li><strong>PDX_API_KEY</strong> to the DataExperience API key from above</li>
  <li><strong>PDX_SECRET</strong> to the DataExperience Secret key from above</li>
  <li><strong>SDK_URL</strong> the Spectrum Geocoding for Big Data product distribution, e.g <b>sbg500F01.zip</b>, where 50 is version 5.0.</li>
</ol>

You will also need to provide the URL for the location of the Spectrum Addressing for Big Data product distribution. You
should be able to find this URL in your product delivery email from Precisely. See these links for information on
creating AWS presigned https://docs.aws.amazon.com/cli/latest/reference/s3/presign.html , and for
Azure https://docs.microsoft.com/en-us/rest/api/storageservices/Service-SAS-Examples?redirectedfrom=MSDN . If you
prefer, you may manually copy the product distribution to your dbfs and use a file:///dbfs/<path_on_dbfs> URL.

Once those changes have been made, you can execute the cell.
The rest of the cells can be executed in order.

<strong> After executing the Create Geocoding Library & Attach to Cluster </strong> cell, you will see two jars called out in the results.  You must pick the appropriate version depending on the spark version you are using.  That jar should be added as a library to your cluster https://docs.databricks.com/libraries/index.html.  The proper version of the jar is... scala 2.11 for Databricks 6, scala 2.12 for Databricks 7 onwards.

## Executing the Demo

After all cells of the Installation have completed successfully, you can move on to Executing the cells in the Demo
workbook.
For this notebook you may need no alterations.
<ol>
  <li><strong>GeocodingRootDBFS</strong> defaults to /geocoding.  Only change if you did so in the Installation notebook as well.</li>
</ol>
You should execute each cell in top-down order.  You will set up the geocode instance, build a simple table of addresses, and then geocode against them.

![Precisely](../Precisely_Logo.png)

# Databricks Geocoding Sample
This sample for Databricks demonstrates how to install, configure, and execute against the appropriate geocoding spark driver. 
After cloning or downloading the repo, you will have 2 html files to be imported into your workspace https://docs.databricks.com/notebooks/notebooks-manage.html.  You will also have /lib/precisely-bigdata-pdx-sdk2.7-full.jar which will need to be added to your Databricks Filestore.

## Data
This sample does not include the data, but does show how to integrate with the Precisely Data Experience.  Using your credentials you will be assured to be using the most recent data vintage.  This also expedites the setting up of data on your cluster.

* **Geocoding Reference Data**: ex: USA-MasterLocationData, USA-TomTom. 

## Setting up DataExperience integration
  1) Collect your Data Experience API Key and Secret Key by visiting https://data.precisely.com/autodownload.  You will need these for the Geocoding Installation notebook.
  2) The DataExperience SDK has been prebuilt and included in the repo at /lib/precisely-bigdata-pdx-sdk2.7-full.jar.  It will need to be added to your Databricks FileStore https://docs.databricks.com/data/filestore.html.  You will need to set the location of the file in the Configuration and Variables cell of the Geocode Installation notebook.

## Installing Geocoding
The Geocoding Installation notebook contains the commands necessary to install the geocoding libraries and reference data. The first command provides configuration variables for the notebook and will need to be updated with values specific to your environment. After updating the configuration section, you can execute each cell individually or run the entire notebook to perform the installation.

The Configuration and Variables cell builds a shell script to hold values needed by the other cells. This will be the main cell that will require your edits.
<ol>
  <li><strong>SDM_CLASSPATH</strong> to the FileStore location of the precisely-bigdata-pdx-sdk2.7-full.jar</li>
  <li><strong>PB_API_KEY</strong> to the DataExperience API key from above</li>
  <li><strong>PB_SECRET</strong> to the DataExperience Secret key from above</li>
  <li><strong>SDK_URL</strong> to any valid/accessible URL location of the spectrum-bigdata-geocoding-VERSION.zip</li>
</ol>

You will also need to provide the URL for the location of the spectrum-bigdata-geocoding-VERSION.zip. In our example, we have a mocked up presigned URL to an s3 repository.  See these links for information on creating AWS presigned https://docs.aws.amazon.com/cli/latest/reference/s3/presign.html, and for Azure https://docs.microsoft.com/en-us/rest/api/storageservices/Service-SAS-Examples?redirectedfrom=MSDN.  If you prefer, add the spectrum-bigdata-geocoding-VERSION.zip to your dbfs and use a file:/// URL.

Once those changes have been made, you can execute the cell.

<strong> After executing the Create Geocoding Library & Attach to Cluster </strong> cell, you will see two jars called out in the results.  You must pick the appropriate version depending on the spark version you are using.  That jar should be added as a library to your cluster https://docs.databricks.com/libraries/index.html.

The rest of the cells can be executed in order with no other interaction required.


## Executing the Demo
After all cells of the Installation have completed successfully, you can move on to Executing the cells in the Demo workbook.
For this notebook you may need no alterations.  
<ol>
  <li><strong>GeocodingRootDBFS</strong> defaults to /geocoding.  Only change if you did so in the Installation notebook as well.</li>
</ol>
You should execute each cell in top-down order.  You will set up the geocode instance, build a simple table of addresses, and then geocode against them.

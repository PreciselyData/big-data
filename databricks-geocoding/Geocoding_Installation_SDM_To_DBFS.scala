// Databricks notebook source
// MAGIC %md
// MAGIC # Spectrum Spatial Geocoding for Databricks - Installation

// COMMAND ----------

// MAGIC %md
// MAGIC ## Configure Environment

// COMMAND ----------

// DBTITLE 1,Configuration and Variables
// Alter the following SDK_URL to point to the location where you placed the spectrum-bigdata-geocoding-VERSION.zip. See the README for links to the documentation covering how to create presigned URL's for AWS and Azure.
val SDK_URL = "YOUR_PRESIGNED_AWS_S3_URL_OR_DBFS_ZIP_PATH"

// Update the following with the API and Secret keys for your DataExperience account; these can be generated by visiting https://data.precisely.com/autodownload.
val PDX_API_KEY = "YOUR_PDX_API_KEY"
val PDX_SECRET = "YOUR_PDX_SECRET_KEY"

// We will be installing to the following directory. You can change the directory to suit your environment - you will need to use the same value in the Geocoding Demo Workspace.
val GeocodingRootDBFS = "/geocoding"

// This is the version of the latest vintage whenever updated to current. The required format is "(YEAR.MONTH)".
val DATA_VINTAGE = "2021.3"

// This is the release date of the latest vintage by default "year-month-first date of the month". For example, 2019-12-01.
val DATA_RELEASE_DATE ="2021-03-01"

// Configure the datasets to be downloaded from data.precisely.com.
val SDM_GEOCODING_SPDS = Array(
  "Geocoding MLD US#United States#All USA#Spectrum Platform Data#1.0.0#$DATA_VINTAGE#$DATA_RELEASE_DATE",
  "Geocoding TT Street US#United States#All USA#Spectrum Platform Data#1.0.0#$DATA_VINTAGE#$DATA_RELEASE_DATE"
)

// Local Environment Setup - The remaining lines should not need to be modified
val DBFS_BASE_LOCATION = s"$GeocodingRootDBFS"
val DBFS_SDK_EXTRACT_LOCATION = s"$DBFS_BASE_LOCATION/sdk"
val DBFS_SDK_LOCATION = s"$DBFS_SDK_EXTRACT_LOCATION/spectrum-bigdata-addressing*"
val DBFS_DATA_LOCATION = s"$DBFS_BASE_LOCATION/data"

val LOCAL_DATA_TMP = s"$DBFS_BASE_LOCATION/tmp/data"
val LOCAL_DATA_ZIPPED = s"$LOCAL_DATA_TMP/zip"
val LOCAL_DATA_UNZIPPED = s"$LOCAL_DATA_TMP/unzipped"

// Add the pdx sdk jar from github to your Filestore.
val PDX_SDK_URL="https://raw.githubusercontent.com/PreciselyData/big-data/dev/databricks-geocoding/lib/precisely-bigdata-pdx-sdk3.0.1-full.jar"

val PDX_CLASSNAME="com.precisely.pdx.sdkexample.SampleDemoApp"
val DBFS_PDX_SDK_JAR=s"$DBFS_SDK_EXTRACT_LOCATION/pdx-sdk.jar"

// We did this in any %sh command to ensure variables are available in the environment.
dbutils.fs.put("file:///dbricks_env.sh",s"""#!/bin/bash

export SDK_URL="$SDK_URL"
export PDX_API_KEY=$PDX_API_KEY
export PDX_SECRET=$PDX_SECRET
export DATA_VINTAGE=$DATA_VINTAGE
export DATA_RELEASE_DATE=$DATA_RELEASE_DATE
export DBFS_SDK_EXTRACT_LOCATION=/dbfs$DBFS_SDK_EXTRACT_LOCATION
export DBFS_SDK_LOCATION=/dbfs$DBFS_SDK_LOCATION
export DBFS_DATA_LOCATION=/dbfs$DBFS_DATA_LOCATION
export LOCAL_DATA_TMP=$LOCAL_DATA_TMP
export LOCAL_DATA_ZIPPED=$LOCAL_DATA_ZIPPED
export LOCAL_DATA_UNZIPPED=$LOCAL_DATA_UNZIPPED
export PDX_SDK_URL=$PDX_SDK_URL
export PDX_CLASSNAME=$PDX_CLASSNAME
export DBFS_PDX_SDK_JAR=/dbfs$DBFS_PDX_SDK_JAR
export GEOCODING_SPDS=( ${SDM_GEOCODING_SPDS.map(spd => s"${'"'}$spd${'"'}").mkString(" ")} )

""",true)


// COMMAND ----------

// MAGIC %md ## Install the SDK

// COMMAND ----------

// DBTITLE 1,Install SDK to DBFS
// MAGIC %sh . /dbricks_env.sh
// MAGIC
// MAGIC rm -rf $DBFS_SDK_EXTRACT_LOCATION
// MAGIC mkdir -p $DBFS_SDK_EXTRACT_LOCATION
// MAGIC
// MAGIC if [ ! -z "$SDK_URL" ]
// MAGIC then
// MAGIC   echo "Installing geocoding SDK..."
// MAGIC   curl -o geocoding-sdk.zip "$SDK_URL"
// MAGIC   unzip -d $DBFS_SDK_EXTRACT_LOCATION geocoding-sdk.zip
// MAGIC else
// MAGIC   echo "Not installing geocoding SDK"
// MAGIC fi

// COMMAND ----------

// MAGIC %sh . /dbricks_env.sh
// MAGIC
// MAGIC if [ ! -z "$PDX_SDK_URL" ]
// MAGIC then
// MAGIC   echo "Installing PDX SDK..."
// MAGIC   curl -o $DBFS_PDX_SDK_JAR "$PDX_SDK_URL"
// MAGIC else
// MAGIC   echo "Not installing geocoding SDK"
// MAGIC fi

// COMMAND ----------

// MAGIC %md ## Install the Data

// COMMAND ----------

// DBTITLE 1,Download Geocoding Reference Data
// MAGIC %sh . /dbricks_env.sh
// MAGIC rm -rf $DBFS_DATA_LOCATION
// MAGIC mkdir -p $DBFS_DATA_LOCATION
// MAGIC printf '%s\n' "${GEOCODING_SPDS[@]}" | xargs -P 4 -I {spd} java -cp $DBFS_PDX_SDK_JAR $PDX_CLASSNAME -a $PDX_API_KEY -s $PDX_SECRET -d $DBFS_DATA_LOCATION -dd \"{spd}\"

// COMMAND ----------

// DBTITLE 1,Create Geocoding Library & Attach to Cluster
// MAGIC %sh . /dbricks_env.sh
// MAGIC
// MAGIC echo "If the SDK has been installed or updated, you should now create the Databricks library and attach it to your cluster."
// MAGIC echo ""
// MAGIC echo "Choose one of the jars below based on the spark/scala version of your cluster: Databricks 6 = Scala 2.11, Databricks 7 = Scala 2.12"
// MAGIC
// MAGIC ls $DBFS_SDK_LOCATION/spark2/driver*/spectrum-bigdata-addressing-spark*-all.jar | sed 's/\/dbfs/dbfs:/'

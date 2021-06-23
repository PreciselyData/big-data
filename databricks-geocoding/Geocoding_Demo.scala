// Databricks notebook source
// MAGIC %md
// MAGIC # Geocoding Demo
// MAGIC In this notebook the geocoder will be configured, some sample data will be created, and geocodes will be executed using the data.
// MAGIC Before executing this notebook make sure to have completed executing all cells in Geocoding Installation notebook.

// COMMAND ----------

// DBTITLE 1,Global Variables
// GeocodingRootDBFS will be the same path specified in the installation script
val GeocodingRootDBFS = "/geocoding"

// where to write the generated preferences file
val PreferencesFileDBFS = s"$GeocodingRootDBFS/geocodePreferences.xml"

// These should not need to be modified
val SDKLocationLocal = s"/dbfs/$GeocodingRootDBFS/sdk"
val PreferencesFileLocal = s"/dbfs$PreferencesFileDBFS"
val DataLocationLocal = s"/dbfs$GeocodingRootDBFS/data"
val ExtractLocationLocal = "/precisely/data"

// COMMAND ----------

// MAGIC %md
// MAGIC ## Geocode Preferences

// COMMAND ----------

// DBTITLE 1,Update Geocoder Preferences
dbutils.fs.put(PreferencesFileDBFS,"""
<geocodePreferences>
    <!--*** To set system wide preferences, uncomment the appropriate lines and insert the desired default value -->
    <!--*** Please see GGS documentation for more detailed information about preferences and their impact. -->
    <!--
        matchMode - Match modes determine the leniency used to make a match
        between the input address and the reference data. Select a match
        mode based on the quality of your input and your desired output.
        Default: STANDARD
        Available Values:
        - STANDARD
          - (Default) Requires a close match and generates a moderate number of match candidates.
        - EXACT
          - Requires a very tight match.
        - RELAXED
          - Allows a loose match and generates the most match candidates.
        - INTERACTIVE
          - (USA Only) Available in single-line address matching only. This mode is designed to better handle
            the specific  matching challenges presented by interactive matching.
        - CASS
          - (USA only) Imposes additional rules to ensure compliance with the USPS CASS regulations.
    -->
    <!-- <matchMode>RELAXED</matchMode> -->
    <!--reverseGeocodeSearchDistance - Sets the radius in which the Reverse Geocode service searches for a
        match to the input coordinates.
        Default: 150
    -->
    <!-- <reverseGeocodeSearchDistance>150</reverseGeocodeSearchDistance> -->
    <!--
        reverseGeocodeSearchDistanceUnits - Specifies the unit of measurement for the search distance.
        Default: METER
        Available Values:
        - FEET
        - METERS
        - MILES
        - KILOMETERS
    -->
    <!-- <reverseGeocodeSearchDistanceUnits>METER</reverseGeocodeSearchDistanceUnits> -->
    <!--
        fallbackToPostalCentroidOnNoStreetCloseMatch - Specifies whether to attempt to determine a post code centroid
        when an address-level geocode cannot be determined
        Default: true
        Available Values:
        - true
        - false
    -->
    <!-- <fallbackToPostalCentroidOnNoStreetCloseMatch>true</fallbackToPostalCentroidOnNoStreetCloseMatch> -->
    <!--
        fallbackToGeographicCentroidOnNoStreetCloseMatch - Specifies whether to attempt to determine a geographic region
        centroid when an address-level geocode cannot be determined.
        Default: true
        Available Values:
        - true
        - false
    -->
    <!-- <fallbackToGeographicCentroidOnNoStreetCloseMatch>true</fallbackToGeographicCentroidOnNoStreetCloseMatch> -->
    <!--
        returnAllAvailableCustomFields - return all custom fields for the candidate
        Default: false
        Available Values:
        - true (will add overhead and might hinder performance)
        - false
     -->
    <!-- <returnAllAvailableCustomFields>false</returnAllAvailableCustomFields> -->
    <!--
        returnAllCandidateInfo - returns all available information for the candidate
        Default: false
        Available Values:
        - true (will add overhead and might hinder performance)
        - false
    -->
    <!-- <returnAllCandidateInfo>true</returnAllCandidateInfo> -->
    <!--
        returnedCustomFieldKey - Specifies a list of keys that represent the custom fields to be
        returned in the candidate's customFields output. For example: "PB_KEY".
        Note: This preference is not necessary if "returnAllCandidateInfo" or "returnAllAvailableCustomFields"
        is "true". Also, to specify multiple keys create multiple xml tags for "<returnedCustomFieldKey/>"
    -->
    <returnedCustomFieldKey>PB_KEY</returnedCustomFieldKey>
    <!--
        streetOffset - Indicates the offset distance from the street segments to use in street-level geocoding.
        Default: 7
    -->
    <!-- <streetOffset>7</streetOffset> -->
    <!--
        streetOffsetUnits - Specifies the unit of measurement for the street offset.
        Default: METERS
        Available Values:
        - FEET
        - METERS
        - MILES
        - KILOMETERS
    -->
    <!-- <streetOffsetUnits>METERS</streetOffsetUnits> -->
    <!--
        cornerOffset - Specifies the distance to offset the street end points in street-level matching.
        Default: 7
    -->
    <!-- <cornerOffset>7</cornerOffset> -->
    <!--
        cornerOffsetUnits - Specifies the unit of measurement for the corner offset.
        Default: METERS
        Available Values:
        - FEET
        - METERS
        - MILES
        - KILOMETERS
    -->
    <!-- <cornerOffsetUnits>METERS</cornerOffsetUnits> -->
    <!-- ********************************INFREQUENTLY USED PREFERENCES BELOW *************************************** -->
    <!--
        customKeyValuePair - Specifies list of key value pairs for custom preferences. Please see documentation for
        full list of "customPreferences"
    -->
    <!--<customKeyValuePair>FALLBACK_TO_WORLD=true</customKeyValuePair>-->
    <!--
        returnedPointCoordinateSystem - Specifies the coordinate system that you want to convert the
        geometry to.
        Note: Specify the coordinate reference system in the format codespace:code, eg WGS84 is
        represented as epsg:4326
        Default: epsg:4326
    -->
    <!-- <returnedPointCoordinateSystem>epsg:4326</returnedPointCoordinateSystem> -->
    <!--
        clientLocale - This field is used for a country that has multiple languages to determine the preferred order
        of language candidates. The locale must be specified in the format "cc_CC", where "cc" is the language and "CC"
        is the ISO 3166-1 Alpha-2 code, such as: en-US, fr_CA or fr_FR.
    -->
    <!-- <clientLocale>en_US</clientLocale> -->
    <maxReturnedCandidates>10</maxReturnedCandidates>
    <!--
        maxCandidateRangesToReturn - Specifies maximum number of ranges returned for a candidate
        Default: 1
    -->
    <!-- <maxCandidateRangesToReturn>1</maxCandidateRangesToReturn> -->
    <!--
        maxCandidateRangeUnitsToReturn - Specifies maximum number of units returned for a range
        Default: 1
    -->
    <!-- <maxCandidateRangeUnitsToReturn>1</maxCandidateRangeUnitsToReturn> -->
    <!-- *********************************************************************************************************** -->
    <!-- *** NOTE mustMatch values (fieldsMatching) is not currently supported in geocodePreferences.xml -->
</geocodePreferences>
""", true)
dbutils.fs.head(PreferencesFileDBFS, 1024*1024*10)

// COMMAND ----------

// DBTITLE 1,Register Geocode Function
import com.pb.bigdata.geocoding.spark.api.GeocodeUDFBuilder
import org.apache.spark.sql.functions._
import java.io.File

def getListOfSubDirectories(dir: File): List[String] = dir.listFiles.filter(_.isDirectory).map(_.getName).toList
val geocodingDirName = getListOfSubDirectories(new File(SDKLocationLocal)).filter(_.startsWith("spectrum-bigdata-geocoding"))(0)
val ResourcesLocationLocal = s"$SDKLocationLocal/$geocodingDirName/resources/"

GeocodeUDFBuilder.singleCandidateUDFBuilder()
				.withResourcesLocation(ResourcesLocationLocal)
                .withDataLocations(DataLocationLocal)
                .withExtractionLocation(ExtractLocationLocal)
				.withPreferencesFile(PreferencesFileLocal)
                .withOutputFields("precisionCode", "PB_KEY", "formattedStreetAddress", "formattedLocationAddress", "x", "y")
				.withErrorField("error")
				.register("geocode", spark);

// COMMAND ----------

// MAGIC %md
// MAGIC ## Build some test data

// COMMAND ----------

var df = Seq(
  ("350 jordan rd","troy","ny","12180"),
  ("1 Global View","troy","ny","12180"),
  ("222 Jersey City Blvd", "Jersey City", "NJ", "7305")
).toDF("address", "city", "state", "postcode")

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Execute the Geocode
// MAGIC Note: Due to the behavior of the spark query execution planner, the geocode function could be executed multiple times for every record.  Because the geocode operation can be computationally expensive, this should be avoided.  Adding a call to "df.persist()" directly after the geocode function should ensure that each record calls the geocode function only once.

// COMMAND ----------

// DBTITLE 1,Run the Geocode Query
df = df.withColumn("geocode_result",
       callUDF("geocode",
        map(
          lit("mainAddressLine"), $"address",
          lit("areaName3"), $"city",
          lit("areaName1"), $"state",
          lit("postCode1"), $"postcode",
          lit("country"), lit("USA")
        )
      )
	).persist()
	.select("*", "geocode_result.*").drop("geocode_result")

display(df)


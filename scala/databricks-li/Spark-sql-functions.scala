// Databricks notebook source
import com.precisely.bigdata.li.spark.api.udf.SQLRegistrator
import org.apache.spark.sql.SparkSession

var sparkSession = SparkSession.builder()
    .appName("sppark-sql-functions")
    .getOrCreate()
SQLRegistrator.registerAll()

// COMMAND ----------

// MAGIC %md
// MAGIC ## **Read Input**

// COMMAND ----------

// DBTITLE 1,Input
var poly_file_path = ""  // the input file path to the csv file containing polygon geometry.
var df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(poly_file_path);
df.createOrReplaceTempView("polygontest")

// COMMAND ----------

// MAGIC %md
// MAGIC ## **Constructor and Persistence **Functions****

// COMMAND ----------

// Geometry from WKT String
var geoFromWKT = spark.sql("SELECT ST_GeomFromWKT(WKT) as Geometry, State_Name, State FROM polygontest")

// Geometry from GeoJSON String
var GeoFromGeoJSON = spark.sql("SELECT ST_GeomFromGeoJSON(ST_ToGeoJSON(ST_GeomFromWKT(WKT))) as Geometry, State_Name, State FROM polygontest")

// Geometry from WKB String
var GeoFromWKB = spark.sql("SELECT ST_GeomFromWKB(ST_ToWKB(ST_GeomFromWKT(WKT))) as Geometry, State_Name, State FROM polygontest")

// Geometry from KML
var GeoFromKML = spark.sql("SELECT ST_GeomFromKML(ST_ToKML(ST_GeomFromWKT(WKT))) as Geometry, State_Name, State FROM polygontest")

// Geometry from Point
var GeoFromPoint = spark.sql("SELECT ST_Point(-73.750333 , 42.736103) as Geometry")

// COMMAND ----------

// MAGIC %md
// MAGIC ## **Predicate Functions**

// COMMAND ----------

// Prepare two views
val Array(df1, df2) = df.randomSplit(Array(0.5, 0.5))
df1.createOrReplaceTempView("geometry1")
df2.createOrReplaceTempView("geometry2")

// The Disjoint function tests if two geometry objects have no points in common.
var disjoint = spark.sql("SELECT ST_Disjoint(ST_GeomFromWKT(t1.WKT), ST_GeomFromWKT(t2.WKT)) as IsDisjoint FROM geometry1 t1, geometry2 t2");

// The Intersects function determines whether or not one geometry object intersects another geometry object.
var intersect = spark.sql("SELECT ST_Intersects(ST_GeomFromWKT(t1.WKT), ST_GeomFromWKT(t2.WKT)) as Intersect FROM geometry1 t1, geometry2 t2");

// The Overlaps function determines whether or not one geometry object overlaps another geometry object.
var overlap = spark.sql("SELECT ST_Overlaps(ST_GeomFromWKT(t1.WKT), ST_GeomFromWKT(t2.WKT)) as Overlap FROM geometry1 t1, geometry2 t2");

// The Within function returns whether or not one geometry object is entirely within another geometry object.
var within = spark.sql("SELECT ST_Within(ST_GeomFromWKT(t1.WKT), ST_GeomFromWKT(t2.WKT)) as Within FROM geometry1 t1, geometry2 t2");

// The IsNullGeometry function performs a null check of the input geometry.
var nullGeo = spark.sql("SELECT ST_IsNullGeom(ST_GeomFromWKT(t1.WKT)) as IsNullGeometry FROM geometry1 t1");

// COMMAND ----------

// MAGIC %md
// MAGIC ## **Measurement **Functions****

// COMMAND ----------

// The Area function calculates and returns the area of given Geometry in the desired unit.
val getAreaInMile = spark.sql("SELECT ST_Area(ST_GeomFromWKT(WKT), 'sq mi', 'SPHERICAL') as Calculated_Area_Mile FROM polygontest")Z
val getAreaInKM = spark.sql("SELECT ST_Area(ST_GeomFromWKT(WKT), 'sq km', 'SPHERICAL') as Calculated_Area_KM FROM polygontest")

// The Distance function calculates and returns the distance between two geometries.
val getDistance = spark.sql("SELECT ST_Distance(ST_GeomFromWKT(t1.WKT), ST_GeomFromWKT(t2.WKT), 'm', 'SPHERICAL') FROM geometry1 t1, geometry2 t2")

//The Length function calculates and returns the geographic length of a line or polyline geometry object in the desired unit type. 
val getLengthInKM = spark.sql("SELECT ST_Length(ST_GeomFromWKT(WKT), 'm', 'CARTESIAN') as Calculated_Length_KM FROM polygontest")

//The Perimeter function calculates and returns the total perimeter of a given geometry in the desired unit type. 
val getPerimeter = spark.sql("SELECT ST_Perimeter(ST_GeomFromWKT(WKT), 'km', 'SPHERICAL') as Calculated_Perimeter_KM FROM polygontest")

// COMMAND ----------

// MAGIC %md
// MAGIC ## **Processing Functions**

// COMMAND ----------

// The Buffer function returns an instance of WritableGeometry having a MultiPolygon geometry inside it which represents a buffered distance around another geometry object.
val getBuffer = spark.sql("SELECT ST_Buffer(ST_GeomFromWKT(WKT), 5.0 , 'km', 4, 'SPHERICAL') as Calculated_Buffer_KM FROM polygontest")

// The ConvexHull function computes the convex hull of a geometry. The convex hull is the smallest convex geometry that contains all the points in the input geometry.
val convexhull = spark.sql("SELECT ST_ConvexHull(ST_GeomFromWKT(WKT)) as Convex_Hull FROM polygontest")

// The Intersection function is a geometry (point, line, or curve) common in two geometry objects (such as lines, curves, planes, and surfaces). It returns the geometry consisting of direct positions that lie in both specified geometries.
var intersection = spark.sql("SELECT ST_Intersection(ST_GeomFromWKT(t1.WKT), ST_GeomFromWKT(t2.WKT)) as Intersection FROM geometry1 t1, geometry2 t2");

// The Transform function transforms a given geometry from one coordinate system to another.
val tranform = spark.sql("SELECT ST_Transform(ST_GeomFromWKT(WKT), 'epsg:3857') as Tranform FROM polygontest")

// The Union function returns a geometry object which represents the union of two input geometry objects.
var union = spark.sql("SELECT ST_Intersection(ST_GeomFromWKT(t1.WKT), ST_GeomFromWKT(t2.WKT)) as Union FROM geometry1 t1, geometry2 t2");

// COMMAND ----------

// MAGIC %md
// MAGIC ## Observer Functions

// COMMAND ----------

// The ST_X function returns the X ordinate of the geometry if the geometry is a point, or Null if the geometry is not a point or is null.
var ST_X = spark.sql("SELECT ST_X(ST_Point(-73.750333 , 42.736103)) as X_Cordinate")

// The ST_XMax function returns the X maxima of a geometry, or Null if the specified value is not a geometry.
var  ST_XMax = spark.sql("SELECT  ST_XMax(ST_Point(33.750333 , 42.736103)) as X_Max_Cordinate")

// The ST_XMin function returns the X minima of a geometry, or Null if the specified value is not a geometry.
var  ST_XMin = spark.sql("SELECT  ST_XMin(ST_Point(33.750333 , 42.736103)) as X_Min_Cordinate")

// The ST_Y function returns the Y ordinate of the geometry if the geometry is a point, or Null if the geometry is not a point or is null.
var ST_Y = spark.sql("SELECT ST_Y(ST_Point(-73.750333 , 42.736103)) as Y_Cordinate")

// The ST_YMax function returns the Y maxima of a geometry, or Null if the specified value is not a geometry.
var  ST_YMax = spark.sql("SELECT  ST_YMax(ST_Point(33.750333 , 42.736103)) as Y_Max_Cordinate")

// The ST_YMin function returns the Y minima of a geometry, or Null if the specified value is not a geometry.
var  ST_YMin = spark.sql("SELECT  ST_YMin(ST_Point(33.750333 , 42.736103)) as Y_Min_Cordinate")

// COMMAND ----------

// MAGIC %md
// MAGIC ## **Grid Function**

// COMMAND ----------

// The GeoHashID function returns a unique well-known string ID for the grid cell. 
// The ID then is sortable and searchable that corresponds to the specified X, Y, and precision.
var  GeoHashId = spark.sql("SELECT ST_GeoHash(ST_Point(-73.750333, 42.736103),3) as GeoHashId")

// The GeoHashBoundary function returns a WritableGeometry that defines the boundary of a cell in a grid if given a unique ID for the location. 
// It also can return the boundary of the cell that contains the given point at the specified precision. The shape of the cell is rectangular.
var  GeoHashBoundary = spark.sql("SELECT ST_GeoHashBoundary('dre') as GeoHashBoundary")

// The HexagonID function returns a unique well-known string ID for the grid cell. 
// The ID then is sortable and searchable that corresponds to the specified X, Y, and precision.
var  HexHash = spark.sql("SELECT ST_HexHash(ST_Point(-73.750333, 42.736103),3) as HexHash")

// The HexagonBoundary function returns a WritableGeometry that defines the boundary of a cell in a grid if given a unique ID for the location. 
// It also can return the boundary of the cell that contains the given point at the specified precision. The shape of the cell is a hexagon.
var  HexagonBoundary = spark.sql("SELECT ST_HexHashBoundary('PF704') as HexagonBoundary")

// The SquareHashID function takes a longitude, latitude (in WGS 84) and a precision. 
// The precision determines how large the grid cells are (higher precision means smaller grid cells). 
// It returns the string ID of the grid cell at the specified precision that contains the point. Square hash cells appear square when displayed on a Popular Mercator map.
var  SquareHash = spark.sql("SELECT ST_SquareHash(ST_Point(-73.750333, 42.736103),3) as SquareHash")

// The SquareHashBoundary function returns a WritableGeometry that defines the boundary of a cell in a grid if given a unique ID for the location. 
// It also can return the boundary of the cell that contains the given point at the specified precision. 
// Square hash cells appear square when displayed on a Popular Mercator map.
var  SquareHashBoundary = spark.sql("SELECT ST_SquareHashBoundary('030') as SquareHashBoundary")

// COMMAND ----------



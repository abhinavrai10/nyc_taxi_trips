import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import LongType
from pyspark.sql.functions import monotonically_increasing_id, col, year, month, dayofmonth, dayofweek, quarter, max, lit, coalesce, when

# Initialize Spark and Glue contexts
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

def get_max_id(table_path: str, id_column: str) -> int:
    """
    Retrieve the maximum ID from a specified column in a Parquet table.
    
    Args:
        table_path (str): S3 path to the Parquet table.
        id_column (str): Name of the ID column to query.
    
    Returns:
        int: Maximum ID value, or 0 if the table doesn't exist.
    """
    try:
        df = spark.read.parquet(table_path)
        max_id = df.select(max(col(id_column))).collect()[0][0]
        return max_id if max_id is not None else 0
    except AnalysisException:
        return 0  # Table doesn't exist
    except Exception as e:
        raise Exception(f"Error reading {table_path}: {str(e)}")

# Load existing fact_trips and get max pickup_date
try:
    fact_trips_existing = spark.read.parquet("s3://lakehouse-nyc-taxi/gold/fact_trips/")
    max_date = fact_trips_existing.select(max(col("pickup_date"))).collect()[0][0]
except AnalysisException:
    max_date = None  # Process all Silver data if fact_trips doesn't exist
except Exception as e:
    raise Exception(f"Error reading fact_trips: {str(e)}")

# Load Silver data and filter incrementally
df_silver = spark.read.parquet("s3://lakehouse-nyc-taxi/silver/yellow_taxi/*/*/")
if max_date is not None:
    df_silver = df_silver.filter(col("pickup_date") > max_date)

# Create or update dim_date dimension
try:
    dim_date_existing = spark.read.parquet("s3://lakehouse-nyc-taxi/gold/dim_date/").cache()
    max_date_id = get_max_id("s3://lakehouse-nyc-taxi/gold/dim_date/", "date_id")
except AnalysisException:
    dim_date_existing = None
    max_date_id = 0
dim_date_new = df_silver.select(
    col("pickup_date").alias("pickup_date")
).distinct().withColumn(
    "year", year("pickup_date")
).withColumn(
    "month", month("pickup_date")
).withColumn(
    "day", dayofmonth("pickup_date")
).withColumn(
    "day_of_week", dayofweek("pickup_date")
).withColumn(
    "quarter", quarter("pickup_date")
).withColumn(
    "date_id", monotonically_increasing_id() + max_date_id + 1
)
dim_date = dim_date_existing.unionByName(dim_date_new, allowMissingColumns=True).distinct() if dim_date_existing else dim_date_new
dim_date.show(5)
dim_date.write.format("parquet").mode("append").option("compression", "snappy").save("s3://lakehouse-nyc-taxi/gold/dim_date/")
dim_date.unpersist() if dim_date_existing else None

# Create or update dim_time dimension
try:
    dim_time_existing = spark.read.parquet("s3://lakehouse-nyc-taxi/gold/dim_time/").cache()
    max_time_id = get_max_id("s3://lakehouse-nyc-taxi/gold/dim_time/", "time_id")
except AnalysisException:
    dim_time_existing = None
    max_time_id = 0
dim_time_new = df_silver.select(
    col("pickup_hour").alias("pickup_hour")
).distinct().withColumn(
    "time_id", monotonically_increasing_id() + max_time_id + 1
)
dim_time = dim_time_existing.unionByName(dim_time_new, allowMissingColumns=True).distinct() if dim_time_existing else dim_time_new
dim_time.orderBy("time_id").show(5)
dim_time.write.format("parquet").mode("append").option("compression", "snappy").save("s3://lakehouse-nyc-taxi/gold/dim_time/")
dim_time.unpersist() if dim_time_existing else None

# Create or update dim_location dimension
try:
    dim_location_existing = spark.read.parquet("s3://lakehouse-nyc-taxi/gold/dim_location/").cache()
    max_location_id = get_max_id("s3://lakehouse-nyc-taxi/gold/dim_location/", "location_id")
except AnalysisException:
    dim_location_existing = None
    max_location_id = 0
locations = df_silver.select(
    col("pickup_location_id").alias("location_id")
).union(
    df_silver.select(col("dropoff_location_id").alias("location_id"))
).distinct()
airport_flags = df_silver.groupBy("pickup_location_id").agg(
    (max("airport_fee") > 0).alias("is_airport_pickup")
).withColumnRenamed("pickup_location_id", "location_id").union(
    df_silver.groupBy("dropoff_location_id").agg(
        (max("airport_fee") > 0).alias("is_airport_pickup")
    ).withColumnRenamed("dropoff_location_id", "location_id")
).groupBy("location_id").agg(
    max("is_airport_pickup").alias("is_airport")
)
dim_location_new = locations.join(airport_flags, "location_id", "left").fillna(False, subset=["is_airport"])
dim_location = dim_location_existing.unionByName(dim_location_new, allowMissingColumns=True).distinct() if dim_location_existing else dim_location_new
dim_location.show(5)
dim_location.write.format("parquet").mode("append").option("compression", "snappy").save("s3://lakehouse-nyc-taxi/gold/dim_location/")
dim_location.unpersist() if dim_location_existing else None

# Create or update dim_rate_code dimension
try:
    dim_rate_code_existing = spark.read.parquet("s3://lakehouse-nyc-taxi/gold/dim_rate_code/").cache()
    max_rate_code_id = get_max_id("s3://lakehouse-nyc-taxi/gold/dim_rate_code/", "rate_code_id")
except AnalysisException:
    dim_rate_code_existing = None
    max_rate_code_id = 0
dim_rate_code_new = df_silver.select(
    col("rate_code")
).distinct().withColumn(
    "rate_code_id", monotonically_increasing_id() + max_rate_code_id + 1
).filter(col("rate_code").isNotNull())
dim_rate_code = dim_rate_code_existing.unionByName(dim_rate_code_new, allowMissingColumns=True).distinct() if dim_rate_code_existing else dim_rate_code_new
dim_rate_code.show(5)
dim_rate_code.write.format("parquet").mode("append").option("compression", "snappy").save("s3://lakehouse-nyc-taxi/gold/dim_rate_code/")
dim_rate_code.unpersist() if dim_rate_code_existing else None

# Create or update dim_payment_type dimension
try:
    dim_payment_type_existing = spark.read.parquet("s3://lakehouse-nyc-taxi/gold/dim_payment_type/").cache()
    max_payment_type_id = get_max_id("s3://lakehouse-nyc-taxi/gold/dim_payment_type/", "payment_type_id")  # Fixed bug
except AnalysisException:
    dim_payment_type_existing = None
    max_payment_type_id = 0
dim_payment_type_new = df_silver.select(
    col("payment_type")
).distinct().withColumn(
    "is_cancellation",
    when(col("payment_type") == "no_charge", True).otherwise(False)
).withColumn(
    "payment_type_id",
    (monotonically_increasing_id() + lit(max_payment_type_id) + 1).cast(LongType())
).filter(col("payment_type").isNotNull())
dim_payment_type = dim_payment_type_existing.unionByName(dim_payment_type_new, allowMissingColumns=True).distinct() if dim_payment_type_existing else dim_payment_type_new
dim_payment_type.show(5)
dim_payment_type.write.format("parquet").mode("append").option("compression", "snappy").save("s3://lakehouse-nyc-taxi/gold/dim_payment_type/")
dim_payment_type.unpersist() if dim_payment_type_existing else None

# Create or update fact_trips table
try:
    fact_trips_existing = spark.read.parquet("s3://lakehouse-nyc-taxi/gold/fact_trips/").cache()
    max_trip_id = get_max_id("s3://lakehouse-nyc-taxi/gold/fact_trips/", "trip_id")
except AnalysisException:
    fact_trips_existing = None
    max_trip_id = 0
fact_trips = df_silver.join(
    dim_date, df_silver["pickup_date"] == dim_date["pickup_date"], "left"
).select(
    df_silver["*"],
    dim_date["date_id"]
).drop(dim_date["pickup_date"])
fact_trips = fact_trips.join(
    dim_time, fact_trips["pickup_hour"] == dim_time["pickup_hour"], "left"
).select(
    fact_trips["*"],
    dim_time["time_id"]
).drop(dim_time["pickup_hour"])
fact_trips = fact_trips.join(
    dim_rate_code, fact_trips["rate_code"] == dim_rate_code["rate_code"], "left"
).select(
    fact_trips["*"],
    dim_rate_code["rate_code_id"]
).drop(fact_trips["rate_code"])
fact_trips = fact_trips.join(
    dim_payment_type, fact_trips["payment_type"] == dim_payment_type["payment_type"], "left"
).select(
    fact_trips["*"],
    dim_payment_type["payment_type_id"]
).drop(fact_trips["payment_type"])
fact_trips = fact_trips.withColumn("trip_id", monotonically_increasing_id() + max_trip_id + 1)
fact_trips = fact_trips.select(
    "trip_id",
    "date_id",
    "time_id",
    "pickup_location_id",
    "dropoff_location_id",
    "rate_code_id",
    "payment_type_id",
    "passenger_count",
    "trip_distance",
    "trip_duration_minutes",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
    "pickup_date"
)
fact_trips.show(5)
fact_trips.write.format("parquet").mode("append").option("compression", "snappy").partitionBy("pickup_date").save("s3://lakehouse-nyc-taxi/gold/fact_trips/")
fact_trips_existing.unpersist() if fact_trips_existing else None

# Commit the Glue job
job.commit()
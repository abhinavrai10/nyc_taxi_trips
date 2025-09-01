import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql import functions as F
import re

# Initialize Spark and Glue contexts
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Parse input arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'year', 'month'])
year = int(args['year'])
month = int(args['month'])

# Load data from Glue Data Catalog
dyf = glueContext.create_dynamic_frame.from_catalog(
    database='nyc_taxi_db',
    table_name='bronze_yellow_taxi',
    push_down_predicate=f"partition_0='{year}' and partition_1='{month}'"
)
dyf.printSchema()
df = dyf.toDF()
df = df.drop("partition_0", "partition_1", "VendorID", "store_and_fwd_flag")
df.printSchema()

def normalize_column_names(df: DataFrame) -> DataFrame:
    """
    Normalize column names in a PySpark DataFrame to lowercase snake_case
    with specific mappings for NYC taxi datasets.

    Args:
        df (DataFrame): Input Spark DataFrame.

    Returns:
        DataFrame: DataFrame with normalized column names.
    """
    rename_map = {
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "RatecodeID": "rate_code",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id",
        "Airport_fee": "airport_fee"
    }

    def to_snake_case(col_name: str) -> str:
        """Convert mixedCase / PascalCase to snake_case lowercase."""
        s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", col_name)
        s2 = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1)
        return s2.lower()

    df_out = df
    for col in df.columns:
        if col in rename_map:
            new_col = rename_map[col]
        else:
            new_col = to_snake_case(col)
        if col != new_col:
            df_out = df_out.withColumnRenamed(col, new_col)

    return df_out

# Normalize column names
df_normalized = normalize_column_names(df)
df_normalized.printSchema()
df_normalized.count()

def cast_and_clean_taxi_data(df: DataFrame) -> DataFrame:
    """
    Cast NYC Taxi dataset columns to appropriate data types and clean invalid records.
    
    Args:
        df (DataFrame): Input normalized DataFrame.
    
    Returns:
        DataFrame: Cleaned and typed DataFrame.
    """
    df = df.withColumn("pickup_datetime", F.to_timestamp("pickup_datetime")) \
           .withColumn("dropoff_datetime", F.to_timestamp("dropoff_datetime"))
    double_cols = [
        "trip_distance", "fare_amount", "extra", "mta_tax",
        "tip_amount", "tolls_amount", "improvement_surcharge",
        "total_amount", "congestion_surcharge",
        "cbd_congestion_fee", "airport_fee"
    ]
    for col in double_cols:
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast("double"))
    int_cols = ["passenger_count", "rate_code_id",
                "payment_type", "pickup_location_id", "dropoff_location_id"]
    for col in int_cols:
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast("int"))
    if "tip_amount" in df.columns and "payment_type" in df.columns:
        df = df.withColumn(
            "tip_amount",
            F.when(F.col("payment_type") == 2, F.lit(0.0)).otherwise(F.col("tip_amount"))
        )
    key_fields = ["pickup_location_id", "dropoff_location_id",
                  "trip_distance", "pickup_datetime", "dropoff_datetime"]
    df = df.filter(F.col("pickup_datetime") < F.col("dropoff_datetime"))
    df = df.na.drop(subset=key_fields)
    return df

# Clean and cast data types
df_clean = cast_and_clean_taxi_data(df_normalized)
df_clean.count()

def filter_anomalous_trips(df: DataFrame, year: int, month: int) -> DataFrame:
    """
    Filters unrealistic or anomalous values from a TLC dataset and restricts to specified year and month.
    
    Args:
        df (DataFrame): Input cleaned DataFrame.
        year (int): Target year to filter.
        month (int): Target month to filter.
    
    Returns:
        DataFrame: Filtered DataFrame with valid trips.
    """
    df = df.filter((F.year(F.col("pickup_datetime")) == year) & (F.month(F.col("pickup_datetime")) == month))
    df = df.withColumn(
        "trip_duration_minutes",
        (F.unix_timestamp(F.col("dropoff_datetime")) - F.unix_timestamp(F.col("pickup_datetime"))) / 60
    )
    window_spec = Window.partitionBy("pickup_location_id")
    df = df.withColumn(
        "fare_stddev",
        F.stddev(F.col("fare_amount")).over(window_spec)
    )
    df = df.withColumn(
        "fare_upper_bound",
        F.avg(F.col("fare_amount")).over(window_spec) + 3 * F.col("fare_stddev")
    )
    filtered_df = df.filter(
        (F.col("trip_distance") >= 0) & (F.col("trip_distance") <= 100) &
        (F.col("fare_amount") >= 0) &
        (F.col("tip_amount") >= 0) &
        (F.col("total_amount") >= 0) &
        ((F.col("passenger_count") > 0) & (F.col("passenger_count") <= 6) | 
         (F.col("passenger_count") == 0) & (F.col("payment_type") == 3)) &
        (F.col("payment_type").isin(1, 2, 3, 4, 5, 6)) &
        (F.col("payment_type") != 4) &
        (F.col("fare_amount") <= F.col("fare_upper_bound")) &
        (F.col("rate_code").isin(1, 2, 3, 4, 5, 6)) &
        (F.col("trip_duration_minutes") >= 1) & (F.col("trip_duration_minutes") <= 240)
    )
    filtered_df = filtered_df.drop("trip_duration_minutes", "fare_stddev", "fare_upper_bound")
    return filtered_df

# Filter anomalous trips
df_curated = filter_anomalous_trips(df_clean, year=year, month=month)
df_curated.show(5)
df_curated.count()

def add_derived_columns(df: DataFrame) -> DataFrame:
    """
    Adds business-relevant derived columns to a TLC dataset using Spark functions.
    
    Args:
        df (DataFrame): Input filtered DataFrame.
    
    Returns:
        DataFrame: DataFrame with added derived columns.
    """
    df_derived = df.withColumn(
        "trip_duration_minutes",
        F.when(
            (F.unix_timestamp(F.col("dropoff_datetime")) - F.unix_timestamp(F.col("pickup_datetime"))) / 60 >= 0,
            (F.unix_timestamp(F.col("dropoff_datetime")) - F.unix_timestamp(F.col("pickup_datetime"))) / 60
        ).otherwise(None)
    ).withColumn(
        "pickup_hour",
        F.hour(F.col("pickup_datetime"))
    ).withColumn(
        "pickup_date",
        F.to_date(F.col("pickup_datetime"))
    )
    return df_derived

# Add derived columns
df_derived = add_derived_columns(df_curated)
df_derived.printSchema()

def map_categorical_columns(df: DataFrame) -> DataFrame:
    """
    Maps categorical codes to human-readable strings and filters out invalid values.
    
    Args:
        df (DataFrame): Input DataFrame with derived columns.
    
    Returns:
        DataFrame: DataFrame with mapped categorical columns and invalid values filtered.
    """
    df_mapped = df.withColumn(
        "rate_code",
        F.when(F.col("rate_code") == 1, "standard_rate")
         .when(F.col("rate_code") == 2, "jfk")
         .when(F.col("rate_code") == 3, "newark")
         .when(F.col("rate_code") == 4, "nassau_or_westchester")
         .when(F.col("rate_code") == 5, "negotiated_fare")
         .when(F.col("rate_code") == 6, "group_ride")
         .otherwise(None)
    ).withColumn(
        "payment_type",
        F.when(F.col("payment_type") == 1, "credit_card")
         .when(F.col("payment_type") == 2, "cash")
         .when(F.col("payment_type") == 3, "no_charge")
         .when(F.col("payment_type") == 4, "dispute")
         .when(F.col("payment_type") == 5, "unknown")
         .when(F.col("payment_type") == 6, "voided")
         .otherwise(None)
    )
    df_mapped = df_mapped.filter(F.col("rate_code").isNotNull())
    return df_mapped

# Map categorical columns
df_mapped = map_categorical_columns(df_derived)
df_mapped.show(5)
df_mapped.count()

# Check unique year-month combinations
df_mapped.select(F.year(F.col("pickup_date")).alias("year"), 
                 F.month(F.col("pickup_date")).alias("month")).distinct().show()

def write_to_silver_layer(df: DataFrame, base_path: str = "s3://lakehouse-nyc-taxi/silver/yellow_taxi", year: int = 1900, month: int = 21) -> None:
    """
    Writes a DataFrame to the Silver layer as a single Parquet file for a specific year and month.
    
    Args:
        df (DataFrame): Input Spark DataFrame with a 'pickup_date' column.
        base_path (str): Base path for the Silver layer.
        year (int): Target year to filter.
        month (int): Target month to filter.
    """
    output_path = f"{base_path}/{year}/{month}/"
    df.coalesce(1) \
        .write \
        .format("parquet") \
        .mode("overwrite") \
        .save(output_path)

# Write to Silver layer
write_to_silver_layer(df_mapped, year=year, month=month)

# Commit the Glue job
job.commit()
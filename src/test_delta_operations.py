
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructField, StructType, TimestampType
from delta.tables import DeltaTable
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, sum as _sum

# Define schema with TimestampType
schema = StructType([
    StructField("sale_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("amount", IntegerType(), True),
    StructField("sale_date", TimestampType(), True)
])

# Define schema for Gold table
gold_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("total_amount", IntegerType(), True)
])

@pytest.fixture(scope="session")
def spark_session():
    """Fixture to provide a Spark session with Delta Lake support enabled."""
    spark = SparkSession.builder \
        .appName("PySpark Delta Testing") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false" ) \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture(scope="session")
def setup_delta_tables(spark_session):
    """Setup and teardown of Delta tables used in the tests."""
    spark_session.sql("DROP TABLE IF EXISTS sales_bronze")
    spark_session.sql("DROP TABLE IF EXISTS sales_silver")
    spark_session.sql("DROP TABLE IF EXISTS sales_gold")

    data = [(1, 101, 1001, 200, datetime.strptime('2020-08-20 10:00:00', '%Y-%m-%d %H:%M:%S')),
            (2, 102, 1002, 150, datetime.strptime('2020-08-20 10:00:00', '%Y-%m-%d %H:%M:%S'))]

    # Setup Bronze table with date objects
    bronze_path = "/tmp/delta/sales_bronze"
    df = spark_session.createDataFrame(data, schema)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(bronze_path)
    spark_session.sql(f"CREATE TABLE sales_bronze USING DELTA LOCATION '{bronze_path}'")
    spark_session.sql("ALTER TABLE sales_bronze SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.logRetentionDuration' = 'interval 0 seconds', 'delta.deletedFileRetentionDuration' = 'interval 0 seconds')")
    spark_session.sql("VACUUM sales_bronze RETAIN 0 HOURS")

    # Setup Silver table with date objects
    silver_path = "/tmp/delta/sales_silver"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_path)
    spark_session.sql(f"CREATE TABLE sales_silver USING DELTA LOCATION '{silver_path}'")
    spark_session.sql("ALTER TABLE sales_silver SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.logRetentionDuration' = 'interval 0 seconds', 'delta.deletedFileRetentionDuration' = 'interval 0 seconds')")
    spark_session.sql("VACUUM sales_silver RETAIN 0 HOURS")
        
    # Define and set up the Gold table
    gold_path = "/tmp/delta/sales_gold"
    df_empty_gold = spark_session.createDataFrame([], gold_schema)
    df_empty_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_path)
    spark_session.sql(f"CREATE TABLE sales_gold USING DELTA LOCATION '{gold_path}'")
    spark_session.sql("VACUUM sales_gold RETAIN 0 HOURS")

    yield bronze_path, silver_path, gold_path

    # Cleanup after tests
    spark_session.sql("DROP TABLE IF EXISTS sales_bronze")
    spark_session.sql("DROP TABLE IF EXISTS sales_silver")
    spark_session.sql("DROP TABLE IF EXISTS sales_gold")


def simulate_data_flow_to_silver(spark_session, bronze_path, silver_path, starting_from_timestamp):
    """Simulate data updates and new data ingestion using CDF, taking only the last change."""
    updates = [(1, 101, 1001, 250, datetime.strptime('2020-08-21 10:00:00', '%Y-%m-%d %H:%M:%S')),  # Updated amount
               (1, 101, 1001, 260, datetime.strptime('2020-08-21 11:00:00', '%Y-%m-%d %H:%M:%S')),  # Another update
               (3, 103, 1001, 300, datetime.strptime('2020-08-22 10:00:00', '%Y-%m-%d %H:%M:%S'))]  # New sale
    df_updates = spark_session.createDataFrame(updates, schema)
    df_updates.write.format("delta").mode("append").save(bronze_path)

    df_changes = spark_session.read.format("delta") \
                                   .option("readChangeData", "true") \
                                   .option("startingTimestamp", starting_from_timestamp) \
                                   .table("sales_bronze")

    window_spec = Window.partitionBy("sale_id").orderBy(col("sale_date").desc())
    df_latest_changes = df_changes.withColumn("rn", row_number().over(window_spec)) \
                                  .filter("rn = 1") \
                                  .drop("rn")

    silver_table = DeltaTable.forPath(spark_session, silver_path)
    (silver_table.alias("silver")
     .merge(
         df_latest_changes.alias("updates"),
         "silver.sale_id = updates.sale_id")
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())

def simulate_data_flow_to_gold(spark_session, silver_path, gold_path, starting_from_timestamp):
    """Simulate data updates from Silver to Gold table using CDF."""
    df_silver_changes = spark_session.read.format("delta").option("readChangeData", "true").option("startingTimestamp", starting_from_timestamp).table("sales_silver")

    window_spec = Window.partitionBy("sale_id").orderBy(col("sale_date").desc())
    df_silver_latest = df_silver_changes.withColumn("rn", row_number().over(window_spec)).filter("rn = 1").drop("rn")

    df_silver_latest.createOrReplaceTempView("temp_latest_changes")

    
    df_gold_aggregate = spark_session.sql("""
        SELECT customer_id, SUM(amount) AS total_amount
        FROM temp_latest_changes
        GROUP BY customer_id
    """)

    gold_table = DeltaTable.forPath(spark_session, gold_path)
    gold_table.alias("gold").merge(df_gold_aggregate.alias("updates"), "gold.customer_id = updates.customer_id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

def test_read_data(setup_delta_tables, spark_session):
    """Test to ensure data can be read from the Delta table."""
    bronze_path, _, _ = setup_delta_tables
    df = spark_session.read.format("delta").load(bronze_path)
    expected_data = [(1, 101, 1001, 200, datetime.strptime('2020-08-20 10:00:00', '%Y-%m-%d %H:%M:%S')),
                     (2, 102, 1002, 150, datetime.strptime('2020-08-20 10:00:00', '%Y-%m-%d %H:%M:%S'))]
    expected_df = spark_session.createDataFrame(expected_data, schema)
    
    assert df.collect() == expected_df.collect(), "Data read from Delta table does not match expected data"

def test_initial_sync(setup_delta_tables, spark_session):
    bronze_path, silver_path, _ = setup_delta_tables
    df_bronze = spark_session.read.format("delta").load(bronze_path)
    df_silver = spark_session.read.format("delta").load(silver_path)
    
    assert df_bronze.collect() == df_silver.collect(), "Initial data in Silver table does not match Bronze table"


def test_data_propagation_to_silver(setup_delta_tables, spark_session):
    bronze_path, silver_path, _ = setup_delta_tables
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    simulate_data_flow_to_silver(spark_session, bronze_path, silver_path, timestamp)
    df_silver = spark_session.read.format("delta").load(silver_path)
    expected_data = [(1, 101, 1001, 260, datetime.strptime('2020-08-21 11:00:00', '%Y-%m-%d %H:%M:%S')),
                     (2, 102, 1002, 150, datetime.strptime('2020-08-20 10:00:00', '%Y-%m-%d %H:%M:%S')),
                     (3, 103, 1001, 300, datetime.strptime('2020-08-22 10:00:00', '%Y-%m-%d %H:%M:%S'))]
    expected_df = spark_session.createDataFrame(expected_data, schema)
    
    differences = df_silver.subtract(expected_df)
    assert differences.count() == 0, "Data in Silver table does not match expected data."

def test_data_propagation_to_gold(setup_delta_tables, spark_session):
    bronze_path, silver_path, gold_path = setup_delta_tables
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    simulate_data_flow_to_silver(spark_session, bronze_path, silver_path, timestamp)  # Ensure Silver table is updated first
    simulate_data_flow_to_gold(spark_session, silver_path, gold_path, timestamp)
    df_gold = spark_session.read.format("delta").load(gold_path)
    expected_data = [(1001, 560), (1002, 150)]
    expected_df = spark_session.createDataFrame(expected_data, ["customer_id", "total_amount"])
    df_gold.show()
    differences = df_gold.subtract(expected_df)
    assert differences.count() == 0, "Data in Gold table does not match expected aggregated data."

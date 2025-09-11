# Databricks notebook source
# PySpark Code for Databricks - Load CSV and Save as Table

from pyspark.sql import SparkSession
# Initialize Spark Session (usually already available in Databricks as 'spark')
spark = SparkSession.builder.appName("SalesDataProcessing").getOrCreate()

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import col, to_date

# Define schema for better performance and data integrity
schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("region", StringType(), True),
    StructField("sales_rep", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("discount_percent", DoubleType(), True),
    StructField("customer_type", StringType(), True),
    StructField("total_amount", DoubleType(), True)
])

#Extracting CSV from Databricks Volume_____________________step 1__________________________
volume_path = "/Volumes/workspace/development/shop_data/sales_data_databricks.csv"

# Read CSV with schema
df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv(volume_path)

#here some cleaning i am doing
# Convert order_date from string to date type
df = df.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))

# cleaning done Showing the DataFrame____________________step 2_____________________
print("DataFrame Schema:")
df.printSchema()
print("\nFirst 10 rows:")
df.show(10)

#validation_________________________________________step 3__________________________
print("\nRow count:", df.count())



#ingestion________________________________step 4___________________________________
# Method 2: Alternative - Read CSV from DBFS (if using DBFS instead of Volume)
# dbfs_path = "/dbfs/FileStore/shared_uploads/sales_data_databricks.csv"
# df_dbfs = spark.read.option("header", "true").schema(schema).csv(dbfs_path)


# Save DataFrame as a Delta table
table_name = "sales_data"
catalog_name = "workspace"  
schema_name = "development"

# Create the full table name
full_table_name = f"{catalog_name}.{schema_name}.{table_name}"

# Write DataFrame as Delta table
df.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(full_table_name)

print(f"\nTable '{full_table_name}' created successfully!")

# Alternative: Save to a specific location and create external table
# external_table_path = f"/Volumes/{catalog_name}/{schema_name}/your_volume/tables/{table_name}"
# df.write.mode("overwrite").option("path", external_table_path).saveAsTable(full_table_name)

# Verify table creation
print("\nTable information:")
spark.sql(f"DESCRIBE {full_table_name}").show()

# Show sample data from the table
print("\nSample data from the table:")
spark.sql(f"SELECT * FROM {full_table_name} LIMIT 5").show()

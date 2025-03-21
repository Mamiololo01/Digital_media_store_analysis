from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, date_trunc, weekofyear, year, to_date, sum as _sum

# Spark session initialization
spark = SparkSession.builder.appName("Invoice_ETL").getOrCreate()

# ingest the CSV data
invoice_df = spark.read.csv('Data/raw/invoice.csv', header=True, inferSchema=True)

# Check the schema
invoice_df.printSchema() # nulls values are allowed based on the schema

# Read the loaded data (first 10 records)
invoice_df.show(10) 

# Replace empty values in column billing_state with not applicable
transform_df = invoice_df.withColumn("billing_state", when(col("billing_state") == "", "N/A").otherwise(col("billing_state")))
transform_df.show(10)




# # For uniformity of records in billing_postal_code, change datatype to string
# transform_df = transform_df.withColumn("billing_postal_code", col("billing_postal_code").cast("string"))
# transform_df.printSchema()
# invoice_df.printSchema()

# Load transformed data to destination 
transform_df.write.csv('Data/transformed/transformed_invoice.csv', header=True, mode='overwrite')

# Aggregation of sales by day
aggregate_day_df = transform_df.groupBy('invoice_date').agg(_sum('total').alias('daily_sales'))
aggregate_day_df.show(40)

# Aggregation of sales by week
aggregate_week_df = transform_df.groupBy(year(col("invoice_date")).alias("year"), 
                                    weekofyear(col("invoice_date")).alias("week_number")) \
                           .agg(_sum("total").alias("weekly_sales")) \
                           .orderBy("year", "week_number")

# Display the value
aggregate_week_df.show()
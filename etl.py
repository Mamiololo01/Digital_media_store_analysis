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

# Display the value
transform_df.show(10)

# Load transformed data to destination 
transform_df.write.csv('Data/transformed/transformed_invoice.csv', header=True, mode='overwrite')

# Aggregation of sales by day
aggregate_day_df = transform_df.groupBy('invoice_date') \
                               .agg(_sum('total').alias('daily_sales'))

# Display the value
aggregate_day_df.show(40)

# Aggregation of sales by week
aggregate_week_df = transform_df.groupBy(year(col("invoice_date")).alias("year"), 
                                    weekofyear(col("invoice_date")).alias("week_number")) \
                                .agg(_sum("total").alias("weekly_sales")) \
                                .orderBy("year", "week_number")


# Display the value
aggregate_week_df.show()



# Task 2.4 – Data Quality and Validation

# def validate_data(invoice_df):
#     """
#     Perform basic data quality checks on the given DataFrame.
#     """
#     # Check for missing values
#     missing_count = invoice_df.filter(col("Total").isNull()).count()
#     if missing_count > 0:
#         print(f" Warning: {missing_count} rows contain missing values.")

#     # Check for duplicates
#     if invoice_df.count() != invoice_df.dropDuplicates().count():
#         print(" Warning: Duplicate rows detected.")
    

#     print(" Data validation completed.")
def validate_data(invoice_df):
    """
    Automated quality check on the pipeline during ETL phase
    """
    # Check for missing values
    missing_count = invoice_df.filter(col("Total").isNull()).count()
    if missing_count > 0:
        print(f" Warning: {missing_count} rows contain missing values.")
    else:
        print(" No missing values found.")

    # Check for duplicates
    duplicate_count = invoice_df.count() - invoice_df.dropDuplicates().count()
    if duplicate_count > 0:
        print(f" Warning: {duplicate_count} duplicate records detected.")
    elif duplicate_count == 0:
        print(" No duplicate record found.")
    else:
        print(" Error: Unexpected issue while checking for duplicates.")

    print(" Data validation completed.")

# # Apply the function
validate_data(invoice_df)


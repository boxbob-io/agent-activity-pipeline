import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_date, date_format, substring

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "SOURCE_BUCKET", "SOURCE_KEY", "SILVER_BUCKET"]
)

input_path = f"s3://{args['SOURCE_BUCKET']}/{args['SOURCE_KEY']}"
output_path = f"s3://{args['SILVER_BUCKET']}/data/"

print(f"Reading from: {input_path}")
print(f"Writing to: {output_path}")

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(input_path)
)

df = df.withColumn(
    "date",
    to_date(substring(col("`Done On`"), 1, 10), "yyyy-MM-dd")
)

df = (
    df.withColumn("year", date_format(col("date"), "yyyy"))
      .withColumn("month", date_format(col("date"), "MM"))
      .withColumn("day", date_format(col("date"), "dd"))
)

(
    df.write
    .mode("append")
    .partitionBy("year", "month", "day")
    .parquet(output_path)
)

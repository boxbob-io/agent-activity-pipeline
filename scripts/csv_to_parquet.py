import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_date, date_format

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'input_path', 'output_path'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

df = spark.read.option("header", "true").csv(args['input_path'])
df = df.withColumn("date", to_date(substring(col("Done On"), 1, 10), "yyyy-MM-dd"))
df = df.withColumn("year", date_format(col("date"), "yyyy")) \
       .withColumn("month", date_format(col("date"), "MM")) \
       .withColumn("day", date_format(col("date"), "dd"))

df.write.partitionBy("year", "month", "day").mode("append").parquet(args['output_path'])

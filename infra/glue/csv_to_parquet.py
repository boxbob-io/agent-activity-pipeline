import logging
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, dayofmonth, month, to_date, to_timestamp, year
from pyspark.sql.types import StringType, StructField, StructType

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "SOURCE_BUCKET", "SOURCE_KEY", "SILVER_BUCKET"]
)

input_path = f"s3://{args['SOURCE_BUCKET']}/{args['SOURCE_KEY']}"
output_path = f"s3://{args['SILVER_BUCKET']}/data/"
bad_records_path = f"s3://{args['SILVER_BUCKET']}/bad_records/"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.info("Reading from: %s", input_path)
logger.info("Writing to: %s", output_path)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

schema = StructType(
    [
        StructField("Done On", StringType(), True),
        StructField("Extension", StringType(), True),
        StructField("Action", StringType(), True),
        StructField("Details", StringType(), True),
    ]
)

df = (
    spark.read
    .option("header", "true")
    .option("quote", '"')
    .option("escape", '"')
    .option("multiLine", "true")
    .option("mode", "DROPMALFORMED")
    .option("badRecordsPath", bad_records_path)
    .schema(schema)
    .csv(input_path)
)

done_on_ts = to_timestamp(col("`Done On`"))
df = df.withColumn("Done On", done_on_ts)
df = df.withColumn("date", to_date(done_on_ts))

df = (
    df.withColumn("year", year(col("date")))
      .withColumn("month", month(col("date")))
      .withColumn("day", dayofmonth(col("date")))
)

(
    df.write
    .mode("append")
    .partitionBy("year", "month", "day")
    .parquet(output_path)
)

job.commit()

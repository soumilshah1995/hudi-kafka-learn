try:

    import os
    import sys
    import uuid
    import json

    import pyspark
    from pyspark.sql import SparkSession
    from pyspark import SparkConf, SparkContext
    from pyspark.sql.functions import col, asc, desc
    from pyspark.sql.functions import col, to_timestamp, monotonically_increasing_id, to_date, when
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    from pyspark import SparkContext, SparkConf
    from pyspark.streaming import StreamingContext

    from faker import Faker
    from faker import Faker

    import findspark

    print("ok.....")
except Exception as e:
    print("Error : {} ".format(e))

spark_version = '3.3.1'
SUBMIT_ARGS = f'--packages ' \
              f'org.apache.hudi:hudi-spark3.3-bundle_2.12:0.12.1,' \
              f'org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version},' \
              f'org.apache.kafka:kafka-clients:2.8.1 ' \
              f'pyspark-shell'
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
findspark.init()

db_name = "hudidb"
table_name = "kafke_data"
recordkey = 'emp_id'
precombine = 'ts'
path = f"file:///C:/tmp/spark_warehouse/{db_name}/{table_name}"
method = 'upsert'
table_type = "COPY_ON_WRITE"
BOOT_STRAP = "localhost:9092"
TOPIC = "FirstTopic"
hudi_options = {
    'hoodie.table.name': table_name,
    'hoodie.datasource.write.recordkey.field': 'emp_id',
    'hoodie.datasource.write.table.name': table_name,
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.datasource.write.precombine.field': 'ts',
    'hoodie.upsert.shuffle.parallelism': 2,
    'hoodie.insert.shuffle.parallelism': 2
}

spark = SparkSession.builder \
    .master("local") \
    .appName("kafka-example") \
    .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
    .config('className', 'org.apache.hudi') \
    .config('spark.sql.hive.convertMetastoreParquet', 'false') \
    .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
    .config('spark.sql.warehouse.dir', path) \
    .getOrCreate()

kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOT_STRAP) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "latest") \
    .option("includeHeaders", "true") \
    .option("failOnDataLoss", "false") \
    .load()


def process_batch_message(df, batch_id):
    my_df = df.selectExpr("CAST(value AS STRING) as json")
    schema = StructType(
        [
            StructField("emp_id",StringType(),True),
            StructField("employee_name",StringType(),True),
            StructField("department",StringType(),True),
            StructField("state",StringType(),True),
            StructField("salary",StringType(),True),
            StructField("age",StringType(),True),
            StructField("bonus",StringType(),True),
            StructField("ts",StringType(),True),

        ]
    )
    clean_df = my_df.select(from_json(col("json").cast("string"), schema).alias("parsed_value")).select("parsed_value.*")
    if clean_df.count() >0:
        clean_df.write.format("hudi"). \
            options(**hudi_options). \
            mode("append"). \
            save(path)
    print("batch_id : ", batch_id, clean_df.show(truncate=False))

query = kafka_df.writeStream \
    .foreachBatch(process_batch_message) \
    .option("checkpointLocation", "path/to/HDFS/dir") \
    .trigger(processingTime="1 minutes") \
    .start().awaitTermination()

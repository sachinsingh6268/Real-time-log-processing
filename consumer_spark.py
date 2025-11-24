from confluent_kafka import Consumer
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DecimalType,BooleanType,DateType
from pyspark.sql.functions import from_json,col


if __name__ == "__main__":

    spark = SparkSession.builder.appName("KafkaJSON")\
                        .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3,io.delta:delta-core_2.12:2.3.0") \
                        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                        .getOrCreate()

    # Kafka input stream
    df = spark.readStream.format("kafka")\
         .option("kafka.bootstrap.servers","localhost:9092")\
         .option("subscribe","logs_topic")\
         .load()

    # cast the kafka bytes to str
    json_df = df.selectExpr("cast(value as String) as json_str")

    # Defining schema (Define nested schema explicitly)
    agent_schema = StructType([
        StructField("browser", StringType()),
        StructField("os", StringType()),
        StructField("device_type", StringType())
    ])

    geographical_schema = StructType([
        StructField("country", StringType()),
        StructField("state", StringType())
    ])

    schema = StructType([
        StructField("request_id", StringType()),
        StructField("user_id", IntegerType()),
        StructField("session_id", StringType()),
        StructField("ip_address", StringType()),
        StructField("url", StringType()),
        StructField("method", StringType()),
        StructField("status_code", IntegerType()),
        StructField("response_time_ms", DecimalType()),
        StructField("bytes_sent", DecimalType()),
        StructField("user_agent", agent_schema),
        StructField("geo", geographical_schema),
        StructField("traffic_source", StringType()),
        StructField("is_authenticated", BooleanType()),
        StructField("ts", DateType())
    ])

    parsed_df = json_df.select(from_json(col("json_str"),schema).alias("data"))

    # flatten the parsed df
    flatten_df = parsed_df.select(
        col("data.request_id"),
        col("data.user_id"),
        col("data.session_id"),
        col("data.ip_address"),
        col("data.url"),
        col("data.method"),
        col("data.status_code"),
        col("data.response_time_ms"),
        col("data.bytes_sent"),
        col("data.user_agent.browser"),
        col("data.user_agent.os").alias("operating_system"),
        col("data.user_agent.device_type"),
        col("data.geo.country"),
        col("data.geo.state"),
        col("data.traffic_source"),
        col("data.is_authenticated"),
        col("data.ts").alias("date")
    )

    output_path = "/home/sachin/Downloads/Datasets/Output/"

    query = flatten_df.writeStream.format("delta")\
            .outputMode("append")\
            .partitionBy("state")\
            .option("checkpointLocation",output_path+"/checkpoints")\
            .start(output_path+"/cleaned_user_logs")

    query.awaitTermination()


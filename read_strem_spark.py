import os

from pyspark.sql.functions import from_json, col, get_json_object

from schemas.job_schema import debezium_payload_schema
from utils import Utils
from config import config

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPICS = "servidor_spva.spva.job,servidor_spva.spva.user_app"

def main():
    spark = Utils.get_spark_session(config=config)

    df_kafka = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPICS) \
        .option("startingOffsets", "earliest") \
        .load()

    df_value_string = df_kafka.selectExpr("CAST(value AS STRING)")

    df_debezium_payload = df_value_string.select(
        get_json_object(col("value"), "$.payload").alias("payload")
    )

    df_parsed = df_debezium_payload.withColumn(
        "parsed_payload", from_json(col("payload"), debezium_payload_schema)
    )

    df_final = df_parsed.select(
        col("parsed_payload.op").alias("operacao"),
        col("parsed_payload.after.*")
    )

    # 6. Escreva no console
    query = df_final.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
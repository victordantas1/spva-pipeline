from pyspark.sql.functions import get_json_object, col, from_json, concat_ws

from src.config import config
from src.preprocessors import PreprocessorText
from src.utils import Utils
from loguru import logger

def extract_payload(batch_df):

    logger.info('Casting value field')
    df_value_string = batch_df.selectExpr("CAST(value AS STRING)")

    logger.info('Extracting Payload')
    df_debezium_payload = df_value_string.select(
        get_json_object(col("value"), "$.payload").alias("payload")
    )

    table = config['collection']

    logger.info('Transform payload with schema')
    df_parsed = df_debezium_payload.withColumn(
        "parsed_payload", from_json(col("payload"), Utils.get_schema(table)),
    )

    logger.info('Select after field')
    df = df_parsed.select(
        col("parsed_payload.op").alias("operacao"),
        col("parsed_payload.after.*"),
        col("parsed_payload.source.*")
    )

    return df

def transform(batch_df, epoch_id):
    spark = Utils.get_spark_session(config)
    logger.info('Preprocessing text data')

    table = config['collection']

    if table == 'job':
        batch_df = batch_df.withColumn(
            config['text_column'], concat_ws(' ', col('title'), col('description'))
        )

    preprocessor = PreprocessorText(config)
    preprocessor.load_pipeline(spark)

    df_preprocessed = preprocessor.process_df(batch_df)

    df_preprocessed \
        .write \
        .format('mongodb') \
        .mode("append") \
        .option("database", config['db']) \
        .option("collection", config['collection']) \
        .save()
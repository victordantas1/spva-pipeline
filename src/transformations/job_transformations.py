from pyspark.sql.functions import unix_timestamp, col, when

from .commom_transformations import transform as common_transform, extract_payload


def transform(batch_df, epoch_id):
    batch_df = extract_payload(batch_df)
    batch_df = batch_df.withColumn('update_date', when(col('update_date').isNotNull(), unix_timestamp(col('update_date'))))
    common_transform(batch_df, epoch_id)


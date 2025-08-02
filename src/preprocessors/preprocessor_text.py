import os
from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql.functions import concat_ws, col
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover
from src.preprocessors import PreprocessorBase
from src.custom_transformers.transformers import TokenFilterChar, NerTransformer
from src.utils import Utils


class PreprocessorText(PreprocessorBase):
    def __init__(self, config: dict, model_is_trained: bool = True):
        self.tokenizer = RegexTokenizer(inputCol=config['text_column'], outputCol="tokens", pattern="[\\s+|\\W]")
        self.stop_words_remover = StopWordsRemover(inputCol="tokens", outputCol="tokens_clean")
        self.token_filter = TokenFilterChar(inputCol="tokens_clean", outputCol="tokens")
        self.ner_transformer = NerTransformer(inputCol="tokens", outputCol="tokens_ner")
        self.pipeline = Pipeline(stages=[self.tokenizer, self.stop_words_remover, self.token_filter, self.ner_transformer])
        self.pipeline_model = self
        self.model_is_trained = model_is_trained
        self.text_column = config['text_column']
        self.pipeline_model_path = config['pipeline_model_path']

    def fit_pipeline(self, jobs_df: DataFrame):
        logger.info('Fitting pipeline')
        self.pipeline_model = self.pipeline.fit(jobs_df)
        self.save_pipeline()
        logger.info('Finished fitting pipeline')

    def process_df(self, df_text):
        logger.info('Processing df')
        df_text = self.pipeline_model.transform(df_text)
        df_text = df_text.withColumn(self.text_column, concat_ws(' ', col('tokens')))
        logger.info('Finished processing df')
        return df_text

    def save_pipeline(self):
        logger.info('Saving pipeline')
        if self.pipeline_model:
            logger.info(f"Saving pipeline in: {self.pipeline_model_path}")
            self.pipeline_model.write().overwrite().save(self.pipeline_model_path)
        else:
            raise Exception("Don't have pipeline model to save")

    def load_pipeline(self, spark):
        logger.info('Loading pipeline')
        if not self.model_is_trained:
            os.makedirs(self.pipeline_model_path, exist_ok=True)
            self.fit_pipeline(Utils.get_train_df(spark))

        logger.info(f"Loading pipeline from: {self.pipeline_model_path}")
        self.pipeline_model = PipelineModel.load(self.pipeline_model_path)
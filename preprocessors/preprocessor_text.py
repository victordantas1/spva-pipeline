from pyspark.sql.functions import concat_ws, col
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover
from preprocessors import PreprocessorBase
from transformers.transformers import TokenFilterChar, NerTransformer


class PreprocessorText(PreprocessorBase):
    def __init__(self, config: dict):
        self.config = config
        self.tokenizer = RegexTokenizer(inputCol=config['text_column'], outputCol="tokens", pattern="[\\s+|\\W]")
        self.stop_words_remover = StopWordsRemover(inputCol="tokens", outputCol="tokens_clean")
        self.token_filter = TokenFilterChar(inputCol="tokens_clean", outputCol="tokens")
        self.ner_transformer = NerTransformer(inputCol="tokens", outputCol="tokens_ner")
        self.pipeline = Pipeline(stages=[self.tokenizer, self.stop_words_remover, self.token_filter, self.ner_transformer])
        self.pipeline_model = None

    def fit_pipeline(self, jobs_df):
        self.pipeline_model = self.pipeline.fit(jobs_df)

    def process_df(self, df_text):
        text_column = self.config['text_column']
        df_text = df_text.withColumn(text_column, concat_ws(' ', col('title'), col('description')))
        df_text = self.pipeline.transform(df_text)
        df_text = df_text.withColumn(text_column, concat_ws(' ', col('tokens')))
        return df_text

    def save_pipeline(self, path: str):
        if self.pipeline_model:
            print(f"Salvando a pipeline em: {path}")
            self.pipeline_model.write().overwrite().save(path)
        else:
            raise Exception("Não há pipeline treinada para salvar.")

    def load_pipeline(self, path: str):
        print(f"Carregando a pipeline de: {path}")
        self.pipeline_model = PipelineModel.load(path)
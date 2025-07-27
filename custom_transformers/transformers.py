from pyspark.ml.param.shared import HasInputCol, HasOutputCol
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf, col
import spacy
from pyspark.ml import Transformer
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable

def _remove_single_char_tokens_func(tokens):
    if tokens is None:
        return []
    return [t for t in tokens if len(t) > 1]

_filter_udf = udf(_remove_single_char_tokens_func, ArrayType(StringType()))

def extract_entities(text):
    nlp = spacy.load("en_core_web_trf")
    if isinstance(text, list):
        text = " ".join(text)
    if not isinstance(text, str):
        return []
    doc = nlp(text)
    return [str(ent.text) for ent in doc.ents]

_ner_udf = udf(extract_entities, ArrayType(StringType()))

class TokenFilterChar(Transformer, DefaultParamsReadable, DefaultParamsWritable, HasInputCol, HasOutputCol):
    def __init__(self, inputCol=None, outputCol=None):
        super(TokenFilterChar, self).__init__()
        self._setDefault(inputCol=inputCol, outputCol=outputCol)

    def _transform(self, df):
        input_column_name = self.getInputCol()
        output_column_name = self.getOutputCol()
        return df.withColumn(output_column_name, _filter_udf(col(input_column_name)))


class NerTransformer(Transformer, DefaultParamsReadable, DefaultParamsWritable, HasInputCol, HasOutputCol):
    def __init__(self, inputCol=None, outputCol=None):
        super(NerTransformer, self).__init__()
        self._setDefault(inputCol=inputCol, outputCol=outputCol)

    def _transform(self, df):
        input_column_name = self.getInputCol()
        output_column_name = self.getOutputCol()
        return df.withColumn(output_column_name, _ner_udf(col(input_column_name)))
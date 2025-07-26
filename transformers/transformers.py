from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf, col
import spacy
from pyspark.ml import Transformer


class TokenFilterChar(Transformer):
    def __init__(self, inputCol=None, outputCol=None):
        super(TokenFilterChar, self).__init__()
        self.inputCol = inputCol
        self.outputCol = outputCol

    def _transform(self, df):
        def remove_single_char_tokens(tokens):
            return [t for t in tokens if len(t) > 1]

        filter_udf = udf(remove_single_char_tokens, ArrayType(StringType()))
        return df.withColumn(self.outputCol, filter_udf(col(self.inputCol)))


class NerTransformer(Transformer):
    def __init__(self, inputCol=None, outputCol=None):
        super(NerTransformer, self).__init__()
        self.inputCol = inputCol
        self.outputCol = outputCol

    def _transform(self, df):
        nlp = spacy.load("en_core_web_trf")

        def extract_entities(text):
            if isinstance(text, list):
                text = " ".join(text)
            if not isinstance(text, str):
                return []
            doc = nlp(text)
            return [str(ent.text) for ent in doc.ents]

        ner_udf = udf(extract_entities, ArrayType(StringType()))
        return df.withColumn(self.outputCol, ner_udf(col(self.inputCol)))
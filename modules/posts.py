import json
import ast

from textblob import TextBlob
from textblob.exceptions import NotTranslated, TranslatorError

from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType


class PostTransformer(Transformer, HasInputCol, HasOutputCol):
    def __init__(self):
        super().__init__()
        
    def _transform(self, dataframe):
        out_col = self.getOutputCol()
        in_col = self.getInputCol()
        
        def get_content(data):
            contents = [doc.get('content') for doc in data]
            return contents
        
        get_cntn = udf(get_content, ArrayType(StringType()))
        return dataframe.withColumn(out_col, get_cntn(in_col))
  

class TranslateTransformer(Transformer, HasInputCol, HasOutputCol):
    def __init__(self):
        super().__init__()
        
    def _transform(self, dataframe):
        out_col = self.getOutputCol()
        in_col = self.getInputCol()
        
        def translate(p):
            try:
                return str(TextBlob(p).translate(from_lang='pl'))
            except (NotTranslated, TranslatorError) as e:
                return 'Translation error'
        
        def process(data):
            translated = [translate(p) for p in data]
            return translated
        
        process = udf(process, ArrayType(StringType()))
        return dataframe.withColumn(out_col, process(in_col))


class SentenceTransformer(Transformer, HasInputCol, HasOutputCol):
    def __init__(self):
        super().__init__()
        
    def _transform(self, dataframe):
        out_col = self.getOutputCol()
        in_col = self.getInputCol()
        
        def get_content(data):
            sentences = []
            for post in data:
                for sentence in TextBlob(post).sentences:
                    sentences.append(str(sentence))
            return sentences
        
        get_cntn = udf(get_content, ArrayType(StringType()))
        return dataframe.withColumn(out_col, get_cntn(in_col))

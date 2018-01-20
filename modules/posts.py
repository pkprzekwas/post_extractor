# coding=utf-8
import json
import ast
import nltk
from nltk.data import load
nltk.download('tagsets')

from textblob import TextBlob
from textblob.exceptions import NotTranslated, TranslatorError

from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType, MapType, IntegerType, StructType, StructField, DoubleType


class PostTransformer(Transformer, HasInputCol, HasOutputCol):
    """
    Klasa PostTransformer dziedziczy po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol,
    pyspark.ml.param.shared.HasOutputCol. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe.
    Metoda ta wydobywa z kolumny inputCol, z formatu json, content i umieszcza go w kolumnie outputCol w postaci tekstu.
    """
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
    """
    Klasa TranslateTransformer dziedziczy po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol,
    pyspark.ml.param.shared.HasOutputCol. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe.
    Metoda ta tłumaczy tekst zawarty w kolumnie inputCol  z języka polskiego na angielski i umieszcza go w kolumnie
    outputCol.
    """
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
    """
    Klasa SentenceTransformer dziedziczy po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol,
    pyspark.ml.param.shared.HasOutputCol. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe.
    Metoda ta dzieli tekst zawarty w kolumnie inputCol  na zdania i umieszcza go w kolumnie outputCol w postaci
    tablicy tekstów.
    """
    def __init__(self):
        super().__init__()

    def _transform(self, dataframe):
        out_col = self.getOutputCol()
        in_col = self.getInputCol()

        def extract_sentences(data):
            sentences = []
            for post in data:
                for sentence in TextBlob(post).sentences:
                    sentences.append(str(sentence))
            return sentences

        ext_sentn = udf(extract_sentences, ArrayType(StringType()))
        return dataframe.withColumn(out_col, ext_sentn(in_col))

    
class SpeechPartsTransformer(Transformer, HasInputCol, HasOutputCol):
    """
    Klasa SpeechPartsTransformer dziedziczy  po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol,
    pyspark.ml.param.shared.HasOutputCol. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe. Metoda ta z
    listy tekstów zawartych w kolumnie inputCol, zlicza wystąpienia części mowy i wstawia do outputCol w postaci słownika, którego
    kluczem jest tag części mowy wg projektu 'Penn Treebank', a wartością lista ilości wystąpień danej części mowy w danym tekście.
    """
    def __init__(self):
        super().__init__()

    def _transform(self, dataframe):
        out_col = self.getOutputCol()
        in_col = self.getInputCol()
        
        def extract_speech_parts(data):
            speech_parts = {}
            
            for tag in load('help/tagsets/upenn_tagset.pickle').keys():
                if any(c.isalpha() for c in tag):
                    speech_parts[tag]=[]
            
            for data_line in data:
                data_line_tags = {}
                
                for tag_tuple in TextBlob(data_line).tags:
                    if tag_tuple[1] in data_line_tags:
                        data_line_tags[tag_tuple[1]] += 1
                    else:
                        data_line_tags[tag_tuple[1]] = 1
                        
                for tag in speech_parts.keys():
                    if tag not in data_line_tags.keys():
                        speech_parts[tag].append(0)
                    else:
                        speech_parts[tag].append(data_line_tags[tag])
            return speech_parts

        ext_speech_parts = udf(extract_speech_parts, MapType(StringType(), ArrayType(IntegerType())))
        return dataframe.withColumn(out_col, ext_speech_parts(in_col))

    
class SentimentTransformer(Transformer, HasInputCol, HasOutputCol):
    """
    Klasa SentimentTransformer dziedziczy  po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol,
    pyspark.ml.param.shared.HasOutputCol. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe. Metoda ta z
    listy tekstów zawartych w kolumnie inputCol, wylicza sentymenty tekstów i wstawia do outputCol w postaci słownika, którego
    kluczami są polaryzacja oraz subiektywność, a wartościami listy wartości odpowiednich pól sentymentów dla danego tekstu.
    """
    def __init__(self):
        super().__init__()

    def _transform(self, dataframe):
        out_col = self.getOutputCol()
        in_col = self.getInputCol()

        def calculate_sentiment(data):
            sentiments = {'polarity':[], 'subjectivity':[]}
            for data_line in data:
                sentiment = TextBlob(data_line).sentiment
                
                sentiments['polarity'].append(sentiment.polarity)
                sentiments['subjectivity'].append(sentiment.subjectivity)
            return sentiments

        cnt_senti = udf(calculate_sentiment, MapType(StringType(), ArrayType(DoubleType())))
        return dataframe.withColumn(out_col, cnt_senti(in_col))
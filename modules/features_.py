# coding=utf-8
import json
import statistics

from functools import reduce

from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import (
    HasInputCol, HasOutputCol, Param
)
from pyspark.sql.functions import udf
from pyspark.sql.types import (
    ArrayType, StringType, IntegerType, MapType, DoubleType
)


class FeatureTransformer(Transformer, HasInputCol, HasOutputCol):
    """
    Klasa FeatureTransformer dziedziczy po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol,
    pyspark.ml.param.shared.HasOutputCol. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe.
    Metoda ta wydobywa zawartość z kolumny inputCol, z formatu json i umieszcza go w kolumnie outputCol w postaci słownika.
    """
    def __init__(self):
        super().__init__()
        
    def _transform(self, dataframe):

        out_col = self.getOutputCol()
        in_col = self.getInputCol()

        def get_content(data):
            contents = {}
            lines = data.splitlines(keepends=False)

            for line in lines:
                json_line = json.loads(line)
                feature_array = json_line.get('features')

                for element in feature_array:
                    name = element.get('name')
                    value = element.get('value')
                    if name in contents:
                        contents[name].append(value)
                    else:
                        contents[name]=[value]
            
            return contents

        get_cntn = udf(get_content, MapType(StringType(), ArrayType(DoubleType())))
        return dataframe.withColumn(out_col, get_cntn(in_col))

class SelectFeaturesTransformer(Transformer, HasInputCol, HasOutputCol):
    """
    Klasa SelectFeaturesTransformer dziedziczy po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol,
    pyspark.ml.param.shared.HasOutputCol. Klasa ta przyjmuje dodatkowy parametr features, który zawiera listę nazw cech,
    które mają zostać przefiltrowane. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe.
    Metoda ta wydobywa zawartość z kolumny inputCol, z formatu listy tupli i umieszcza go w kolumnie outputCol w postaci
    przefiltrowanego słownika.
    """
    @keyword_only
    def __init__(self, **kwargs):
        super().__init__()
        self.features = Param(self, "features", "")
        self._setDefault(features=set())
        self.set_params(**self._input_kwargs)

    @keyword_only
    def set_params(self, **kwargs):
        return self._set(**self._input_kwargs)
    
    def set_features(self, value):
        self._paramMap[self.features] = value
        return self

    def get_features(self):
        return self.getOrDefault(self.features)    
        
    def _transform(self, dataframe):
        features_to_filter = self.get_features()
        out_col = self.getOutputCol()
        in_col = self.getInputCol()

        def select_features(features):
            selected_features = {key:features[key] for key in features_to_filter if key in features}
            return selected_features

        get_selected_features = udf(select_features, MapType(StringType(), ArrayType(DoubleType())))
        return dataframe.withColumn(out_col, get_selected_features(in_col))
    
class MaxFeaturesTransformer(Transformer, HasInputCol, HasOutputCol):
    """
    Klasa MaxFeaturesTransformer dziedziczy  po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol,
    pyspark.ml.param.shared.HasOutputCol. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe. Metoda
    ta wylicza maksymalne wartości dla cech podanych w parametrze features, ze wszystkich obiektów znajdujących
    się w słowniku zawartym w kolumnie inputCol. Wyliczone wartości wstawia do kolumny outputCol w postaci słownika.
    """
    def _transform(self, dataframe):

        out_col = self.getOutputCol()
        in_col = self.getInputCol()

        def max_features(features):
            
            for key in features:
                features[key]=reduce(lambda a,b: a if a>b else b, features[key])
                
            return features

        max_features_f = udf(max_features, MapType(StringType(), DoubleType()))
        return dataframe.withColumn(out_col, max_features_f(in_col))


class MeanFeaturesTransformer(Transformer, HasInputCol, HasOutputCol):
    """
    Klasa MeanFeaturesTransformer dziedziczy  po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol,
    pyspark.ml.param.shared.HasOutputCol. Klasa ta przyjmuje dodatkowy parametr features, który zawiera listę nazw cech,
    które mają zostać zagregowane. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe. Metoda
    ta wylicza średnie wartości dla cech podanych w parametrze features, ze wszystkich obiektów jsonowych znajdujących
    się w tekście zawartym w kolumnie inputCol. Wyliczone wartości wstawia do kolumny outputCol w postaci listy
    double’i.
    """
    def _transform(self, dataframe):

        out_col = self.getOutputCol()
        in_col = self.getInputCol()

        def mean_features(features):
            
            for key in features:
                features[key]=sum(features[key])/len(features[key])
                
            return features

        mean_features_f = udf(mean_features, MapType(StringType(), DoubleType()))
        return dataframe.withColumn(out_col, mean_features_f(in_col))


class MedianFeaturesTransformer(Transformer, HasInputCol, HasOutputCol):
    """
    Klasa MedianFeaturesTransformer dziedziczy  po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol,
    pyspark.ml.param.shared.HasOutputCol. Klasa ta przyjmuje dodatkowy parametr features, który zawiera listę nazw cech,
    które mają zostać zagregowane. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe. Metoda
    ta wylicza medianę dla cech podanych w parametrze features, ze wszystkich obiektów jsonowych znajdujących się
    w tekście zawartym w kolumnie inputCol. Wyliczone wartości wstawia do kolumny outputCol w postaci listy double’i.
    """
    def _transform(self, dataframe):

        out_col = self.getOutputCol()
        in_col = self.getInputCol()

        def max_features(features):
            
            for key in features:
                features[key]=statistics.median(features[key])
                
            return features

        max_features_f = udf(max_features, MapType(StringType(), DoubleType()))
        return dataframe.withColumn(out_col, max_features_f(in_col))


class NumberOfOccurrencesFeaturesTransformer(Transformer, HasInputCol, HasOutputCol):
    """
    Klasa MedianFeaturesTransformer dziedziczy  po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol,
    pyspark.ml.param.shared.HasOutputCol. Klasa ta przyjmuje dodatkowy parametr features, który zawiera listę nazw cech,
    które mają zostać zagregowane. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe. Metoda
    ta wylicza medianę dla cech podanych w parametrze features, ze wszystkich obiektów jsonowych znajdujących się
    w tekście zawartym w kolumnie inputCol. Wyliczone wartości wstawia do kolumny outputCol w postaci listy double’i.
    """
    def _transform(self, dataframe):

        out_col = self.getOutputCol()
        in_col = self.getInputCol()

        def count_features(features):
            
            for key in features:
                features[key]=len(features[key])
                
            return features

        count_features_f = udf(count_features, MapType(StringType(), IntegerType()))
        return dataframe.withColumn(out_col, count_features_f(in_col))

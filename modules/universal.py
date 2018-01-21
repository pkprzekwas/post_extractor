# coding=utf-8
import ast
import statistics

from functools import reduce

from textblob import TextBlob
from textblob.exceptions import NotTranslated, TranslatorError

from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, Param
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, DoubleType, StringType, MapType, IntegerType


class ConvertDictToVectorTransformer(Transformer, HasInputCol, HasOutputCol):
    """
    Klasa ConvertDictToVectorTransformer dziedziczy po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol,
    pyspark.ml.param.shared.HasOutputCol. Klasa ta przyjmuje dodatkowe parametry: keys, który zawiera listę nazw kluczy,
    które mają zostać skonwertowane w podanej kolejności do formatu wektora oraz element_type, który określa typ elementu słownika.
    Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe. Metoda ta wydobywa zawartość z kolumny inputCol, w
    formacie słownika i umieszcza go w kolumnie outputCol w postaci wektora wartości kluczy podanych w parametrze. W przypadku braku
    istnienia danego klucza w słowniku, zwracana wartość będzie równa None.
    """
    @keyword_only
    def __init__(self, **kwargs):
        super().__init__()
        self.keys = Param(self, "keys", "")
        self._setDefault(keys=set())
        self.set_params(**self._input_kwargs)

    @keyword_only
    def set_params(self, **kwargs):
        return self._set(**self._input_kwargs)
    
    def set_keys(self, value):
        self._paramMap[self.keys] = value
        return self

    def get_keys(self):
        return self.getOrDefault(self.keys) 
        
    def _transform(self, dataframe):
        keys_to_filter = self.get_keys()
        out_col = self.getOutputCol()
        in_col = self.getInputCol()

        def select_records(dictionary):
            selected_records = []
            for key in keys_to_filter:
                if key in dictionary.keys():
                    selected_records.append(dictionary[key])
                else:
                    selected_records.append(None)
            return Vectors.dense(selected_records)

        get_selected_records = udf(select_records, VectorUDT())
        return dataframe.withColumn(out_col, get_selected_records(in_col))

class SelectRecordsTransformer(Transformer, HasInputCol, HasOutputCol):
    """
    Klasa SelectRecordsTransformer dziedziczy po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol,
    pyspark.ml.param.shared.HasOutputCol. Klasa ta przyjmuje dodatkowe parametry: keys, który zawiera listę nazw kluczy,
    które mają zostać skonwertowane w podanej kolejności do formatu listy oraz element_type, który określa typ elementu słownika.
    Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe. Metoda ta wydobywa zawartość z kolumny inputCol, w
    formacie słownika i umieszcza go w kolumnie outputCol w postaci słownika z przefiltrowanymi kluczami podanymi w parametrze. W
    przypadku braku istnienia danego klucza w słowniku, zwracana wartość będzie równa None.
    """
    @keyword_only
    def __init__(self, **kwargs):
        super().__init__()
        self.keys = Param(self, "keys", "")
        self.element_type = Param(self, "element_type", "")
        self._setDefault(keys=set())
        self._setDefault(element_type=set())
        self.set_params(**self._input_kwargs)

    @keyword_only
    def set_params(self, **kwargs):
        return self._set(**self._input_kwargs)
    
    def set_keys(self, value):
        self._paramMap[self.keys] = value
        return self

    def get_keys(self):
        return self.getOrDefault(self.keys)
    
    def set_element_type(self, value):
        self._paramMap[self.element_type] = value
        return self

    def get_element_type(self):
        return self.getOrDefault(self.element_type)
        
    def _transform(self, dataframe):
        keys_to_filter = self.get_keys()
        element_type = self.get_element_type()
        out_col = self.getOutputCol()
        in_col = self.getInputCol()

        def select_records(dictionary):
            selected_records = {}
            for key in keys_to_filter:
                if key in dictionary:
                    selected_records[key]=dictionary[key]
                else:
                    selected_records[key]=None
                    
            return selected_records

        get_selected_records = udf(select_records, MapType(StringType(), element_type))
        return dataframe.withColumn(out_col, get_selected_records(in_col))
    
class MaxTransformer(Transformer, HasInputCol, HasOutputCol):
    """
    Klasa MaxFeaturesTransformer dziedziczy  po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol,
    pyspark.ml.param.shared.HasOutputCol. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe. Metoda
    ta wylicza maksymalne wartości dla cech podanych w parametrze features, ze wszystkich obiektów znajdujących
    się w słowniku zawartym w kolumnie inputCol. Wyliczone wartości wstawia do kolumny outputCol w postaci słownika.
    """
    def _transform(self, dataframe):

        out_col = self.getOutputCol()
        in_col = self.getInputCol()

        def max_items(dictionary):
            
            for key in dictionary:
                dictionary[key]=reduce(lambda a,b: float(a) if a>b else float(b), dictionary[key])
                
            return dictionary

        max_items_f = udf(max_items, MapType(StringType(), DoubleType()))
        return dataframe.withColumn(out_col, max_items_f(in_col))

class MeanTransformer(Transformer, HasInputCol, HasOutputCol):
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

        def mean_items(dictionary):
            
            for key in dictionary:
                dictionary[key]=sum(dictionary[key])/len(dictionary[key])
                
            return dictionary

        mean_items_f = udf(mean_items, MapType(StringType(), DoubleType()))
        return dataframe.withColumn(out_col, mean_items_f(in_col))

class MedianTransformer(Transformer, HasInputCol, HasOutputCol):
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

        def median_items(dictionary):
            
            for key in dictionary:
                dictionary[key]=statistics.median(dictionary[key])
                dictionary[key]=float(dictionary[key])
                
            return dictionary

        median_items_f = udf(median_items, MapType(StringType(), DoubleType()))
        return dataframe.withColumn(out_col, median_items_f(in_col))

    
class NumberOfOccurrencesTransformer(Transformer, HasInputCol, HasOutputCol):
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

        def count_items(dictionary):
            
            for key in dictionary:
                dictionary[key]=len(dictionary[key])
                
            return dictionary

        count_items_f = udf(count_items, MapType(StringType(), IntegerType()))
        return dataframe.withColumn(out_col, count_items_f(in_col))
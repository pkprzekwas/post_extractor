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
    Metoda ta wydobywa zawartość z kolumny inputCol, z formatu json i umieszcza go w kolumnie outputCol w postaci słownika, którego
    kluczem jest nazwa featura, a wartością lista wartości danego featura, w kolejnych elementach jsonowych.
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
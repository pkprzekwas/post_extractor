import json

from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import (
    HasInputCol, HasOutputCol, Param
)
from pyspark.sql.functions import udf
from pyspark.sql.types import (
    ArrayType, StringType
)


class FeaturesTransformer(Transformer, HasInputCol, HasOutputCol):

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

        features = self.get_features()
        out_col = self.getOutputCol()
        in_col = self.getInputCol()

        def feature_collect(data):
            features_dict = {}
            count = 0

            for feature in features:
                features_dict[feature] = 0
            lines = data.splitlines(keepends=False)

            for line in lines:
                count += 1
                json_line = json.loads(line)
                feature_array = json_line.get('features')

                for element in feature_array:
                    name = element.get('name')
                    if name in features_dict:
                        features_dict[name] += element.get('value')

            values = []
            for feature in features:
                values.append(features_dict[feature] / count)
            return values

        get_cntn = udf(feature_collect, ArrayType(StringType()))
        return dataframe.withColumn(out_col, get_cntn(in_col))

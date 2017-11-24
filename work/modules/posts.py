import ast

from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol
from pyspark.sql.functions import udf
from pyspark.sql.functions import monotonically_increasing_id


class PostTransformer(Transformer, HasInputCol, HasOutputCol):
    def __init__(self):
        super().__init__()
        
    def _transform(self, dataframe):
        out_col = self.getOutputCol()
        in_col = self.getInputCol()
        
        def get_content(data):
            data = ast.literal_eval(data)
            posts = [post.get('content') for post in data]
            return posts
        
        get_cntn = udf(get_content)
        return dataframe.withColumn(out_col, get_cntn(in_col))

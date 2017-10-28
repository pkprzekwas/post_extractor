import conf as c
from data_pipeline import DataPipeline

if __name__ == '__main__':
    pipeline = DataPipeline(data_in_path=c.IN, data_out_path=c.OUT)
    pipeline.start(lang='pl')

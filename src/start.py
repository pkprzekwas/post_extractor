import logging

import conf as c
from data_pipeline import DataPipeline

if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p',
        level=logging.INFO
    )
    pipeline = DataPipeline(data_in_path=c.IN, data_out_path=c.OUT)
    logging.info('Processing starts... (it might take a while)')
    pipeline.start(lang='pl')
    logging.info('All done. Check `./data/out/` directory.')

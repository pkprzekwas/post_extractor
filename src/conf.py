import os
import sys
import logging

dir_ = os.path.dirname(os.path.abspath(__file__))
sys.path.append(dir_)


def logger_conf():
    logging.basicConfig(
        format='%(asctime)s %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p',
        level=logging.INFO
    )


CONFIG = {
    'data_input_directory': dir_ + '/../data/in/',
    'data_output_directory': dir_ + '/../data/out/'
}

logger_conf()

import json

from typing import List

import conf
from data_pipeline import SpeechPartsExtractor
from file_extractor import File
from post import Post
from post_reader import PostReader
from storage import DataStorageService


def get_posts(file):
    """Return posts in JSON format"""
    reader = PostReader(file)
    p = reader.posts
    return p


def extract_posts_from_files(file_list):
    """Return list of JSON posts"""
    posts_as_json = []
    for file in file_list:
        if file is not None:
            posts_as_json.extend(get_posts(file))
    return posts_as_json


if __name__ == '__main__':

    configuration = conf.CONFIG
    storage_service = DataStorageService(configuration)

    files = storage_service.read_all()
    posts = extract_posts_from_files(files)

    pipeline = SpeechPartsExtractor(lang='eng')
    parts_of_speech = pipeline.run(posts)

    storage_service.write(
        file_name='output',
        data=parts_of_speech,
        saver=storage_service.tag_saver
    )

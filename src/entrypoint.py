from functools import partial

import conf
from data_access.post_reader import PostReader
from data_access.storage import DataStorageService
from processing_modules.speech_parts_module import SpeechPartsModule
from processing_modules.sentence_module import SentenceModule
from processing_modules.sentiment_module import SentimentModule


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

    speech_parts = SpeechPartsModule(lang='pl')
    sentence = SentenceModule(lang='pl')
    sentiment = SentimentModule(lang='pl')

    pipeline = [
        speech_parts.run,
        sentence.run,
        sentiment.run
    ]

    results = []

    for callable_ in pipeline:
        results.append(callable_(posts))

    storage_service.write(
        file_name=configuration['extracted_speech_parts'],
        data=results[0],
        saver=storage_service.tag_saver
    )

    storage_service.write(
        file_name=configuration['extracted_sentences'],
        data=results[1],
        saver=storage_service.sentence_saver
    )

    storage_service.write(
        file_name=configuration['extracted_sentiment'],
        data=(results[1], results[2]),
        saver=storage_service.sentiment_saver
    )

import json
import logging
import operator
from typing import List, Tuple, Callable, Iterable, TextIO
from collections import OrderedDict

from post import Post
from post_reader import PostReader
from file_extractor import FileExtractor, File
from textblob import Sentence


class DataPipeline(object):
    def __init__(self, data_in_path, data_out_path):
        self._in = data_in_path
        self._out = data_out_path
        self._files = None
        self._posts = []
        self._translated = []
        self._tags = []
        self._results = OrderedDict()
        self._sentences = []

    def start(self, lang= 'pl'):
        self._files = self.get_files(self._in)
        logging.info('Reading files \t---> started')
        for file in self._files:
            if file is not None:
                self._posts.extend(self.get_posts(file))
        logging.info('Reading files \t---> done')

        logging.info('Translating \t---> started')
        self._posts = self.to_eng(self._posts, lang)
        logging.info('Translating \t---> done')
        for post in self._posts:
            if post is not None:
                self._tags.extend(post.tags)
                self._sentences.extend(post.sentences)
        self._results = self.tags_stats(self._tags)

        logging.info('Saving \t\t---> started')
        self.write_all()
        logging.info('Saving \t\t---> done')

    def write_all(self):
        kwargs_list = [
            dict(
                file_name='out.txt',
                data=self._tags,
                saver=self.tag_saver),
            dict(
                file_name='results.json',
                data=self._results,
                saver=self.result_saver),
            dict(
                file_name='posts.txt',
                data=self._posts,
                saver=self.post_saver),
            dict(
                file_name='sentences.txt',
                data=self._sentences,
                saver=self.sentence_saver)
        ]
        for kwargs in kwargs_list:
            self.write(**kwargs)

    def write(self, file_name: str, data: Iterable, saver: Callable):
        file_path = '/'.join([self._out, file_name])
        with open(file_path, 'w') as f:
            saver(f, data)

    @staticmethod
    def tag_saver(f: TextIO, tags: List[Tuple]):
        for tag in tags:
            f.write('{}\t{}\n'.format(tag[0], tag[1]))

    @staticmethod
    def result_saver(f: TextIO, results: OrderedDict):
        json.dump(results, f)

    @staticmethod
    def post_saver(f: TextIO, posts: List[Post]):
        for post in posts:
            f.write(str(post) + '\n')

    @staticmethod
    def sentence_saver(f: TextIO, sentences: List[Sentence]):
        for sentence in sentences:
            f.write(sentence.string + ' ' + str(sentence.sentiment) +'\n')

    @staticmethod
    def to_eng(posts: list, lang: str) -> List[Post]:
        return [p.translate(lang=lang) for p in posts]

    @staticmethod
    def get_posts(file: File) -> List[Post]:
        reader = PostReader(file)
        posts = reader.posts
        return posts

    @staticmethod
    def get_files(path: str) -> List[File]:
        extractor = FileExtractor(directory=path)
        files = extractor.get_file_list()
        return files

    @staticmethod
    def tags_stats(tags: List[Tuple]) -> OrderedDict:
        types = {}

        for tag in tags:
            if tag[1] in types:
                types[tag[1]] += 1
            else:
                types[tag[1]] = 1

        return OrderedDict(
            reversed(sorted(types.items(), key=operator.itemgetter(1)))
        )

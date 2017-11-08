import json
from collections import OrderedDict

from typing import List, Tuple, Callable, Iterable, TextIO

from post import Post
from file_extractor import FileExtractor, File


class DataStorageService:
    """Helper data acquisition object. Abstract for our data catalog."""

    def __init__(self, conf):
        super().__init__()
        self._data_in = conf['data_input_directory']
        self._data_out = conf['data_output_directory']

    def read_all(self) -> List[File]:
        extractor = FileExtractor(directory=self._data_in)
        files = extractor.get_file_list()
        return files

    def write_all(self, tags, results, posts):
        kwargs_list = [
            dict(
                file_name='out.txt',
                data=tags,
                saver=self.tag_saver),
            dict(
                file_name='results.json',
                data=results,
                saver=self.result_saver),
            dict(
                file_name='posts.txt',
                data=posts,
                saver=self.post_saver
            )
        ]
        for kwargs in kwargs_list:
            self.write(**kwargs)

    def write(self, file_name: str, data: Iterable, saver: Callable):
        file_path = '/'.join([self._data_out, file_name])
        with open(file_path, 'w') as f:
            saver(f, data)

    @staticmethod
    def tag_saver(f, tags):
        json.dump(tags, f)
            #f.write('{}\t{}\n'.format(tag[0], tag[1]))

    @staticmethod
    def result_saver(f: TextIO, results: OrderedDict):
        json.dump(results, f)

    @staticmethod
    def post_saver(f: TextIO, posts: List[Post]):
        for post in posts:
            f.write(str(post) + '\n')

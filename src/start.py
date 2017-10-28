from typing import List, Tuple

import conf as c
from file_extractor import FileExtractor, File
from post_reader import PostReader
from post import Post


class DataPipeline(object):
    def __init__(self, data_in_path, data_out_path):
        self._in = data_in_path
        self._out = data_out_path
        self._files = None
        self._posts = []
        self._translated = []
        self._tags = []

    def start(self, lang: str= 'pl'):
        self._files = self.get_files(self._in)
        for file in self._files:
            self._posts.extend(self.get_posts(file))

        self._posts = self.to_eng(self._posts, lang)
        for post in self._posts:
            self._tags.extend(post.tags)

        self.write_tags()

    def write_tags(self):
        file_name = 'out.txt'
        file_path = '/'.join([self._out, file_name])

        with open(file_path, 'w') as f:
            for tag in self._tags:
                f.write('{}\t{}\n'.format(tag[0], tag[1]))

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


if __name__ == '__main__':
    pipeline = DataPipeline(data_in_path=c.IN, data_out_path=c.OUT)
    pipeline.start(lang='pl')

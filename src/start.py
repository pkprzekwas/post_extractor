import conf as c
from file_extractor import FileExtractor, File
from post_reader import PostReader, Post


class DataPipeline(object):
    def __init__(self, data_path):
        self._data_path = data_path
        self._files = None
        self._posts = []
        self._translated = []

    def start(self, lang: str= 'pl'):
        self._files = self.get_files(self._data_path)

        for file in self._files:
            self._posts.extend(self.get_posts(file))

        self._posts = self.to_eng(self._posts, lang)

        for p in self._posts:
            print(p)
        
    @staticmethod
    def to_eng(posts: list, lang: str):
        return [p.translate(lang=lang) for p in posts]

    @staticmethod
    def get_posts(file: File):
        reader = PostReader(file)
        posts = reader.posts
        return posts

    @staticmethod
    def get_files(path: str):
        extractor = FileExtractor(directory=path)
        files = extractor.get_file_list()
        return files


if __name__ == '__main__':
    dp = DataPipeline(data_path=c.DATA_PATH)
    dp.start(lang='pl')

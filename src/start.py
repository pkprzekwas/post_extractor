import conf as c
from file_extractor import FileExtractor
from post_reader import PostReader


class DataPipeline(object):
    @staticmethod
    def start(data_path):

        extractor = FileExtractor(directory=data_path)
        files = extractor.get_file_list()

        for file_ in files:
            reader = PostReader(file_)
            for post in reader.posts:
                print(post.pl_to_en())


if __name__ == '__main__':
    DataPipeline.start(data_path=c.DATA_PATH)

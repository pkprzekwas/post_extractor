import json

from data_access.file_extractor import FileExtractor


class DataStorageService:
    """Helper data acquisition object. Abstract for our data catalog."""

    def __init__(self, conf):
        super().__init__()
        self._data_in = conf['data_input_directory']
        self._data_out = conf['data_output_directory']

    def read_all(self):
        extractor = FileExtractor(directory=self._data_in)
        files = extractor.get_file_list()
        return files

    def write(self, file_name, data, saver):
        file_path = '/'.join([self._data_out, file_name])
        with open(file_path, 'w') as f:
            saver(f, data)

    @staticmethod
    def tag_saver(f, tags):
        json.dump(tags, f, indent=4)

    @staticmethod
    def sentence_saver(f, sentences):
        out = []
        for sentence in sentences:
            out.append(str(sentence))
        json.dump(out, f, indent=4)

    @staticmethod
    def sentiment_saver(f, data):
        out = {}
        for sentence, sentiment in zip(data[0], data[1]):
            out[str(sentence)] = sentiment
        json.dump(out, f, indent=4)

    @staticmethod
    def post_saver(f, posts):
        for post in posts:
            f.write(str(post) + '\n')

import glob
import json


class FileExtractor(object):
    """ Extracts files of given format from a directory. """
    def __init__(self, directory: str, fmt: str='json'):
        self._directory = directory
        self._format = fmt
        self._pattern = None

    def _get_pattern(self) -> str:
        """ Pattern to match files in a given format. """
        return '/*'.join([self._directory, self._format])

    def get_file_list(self) -> list:

        if not self._pattern:
            self._pattern = self._get_pattern()

        files = glob.glob(self._pattern)
        return [File(f) for f in files]


class File(object):
    def __init__(self, path: str):
        self._path = path
        self._content = None
        self._fmt = None

    def __str__(self):
        return self._path.split('/')[-1]

    def __repr__(self):
        return str(self)

    def _fmt_from_path(self) -> str:
        return self._path.split('.')[-1].lower()

    @property
    def format(self):
        return self._fmt_from_path()

    @property
    def content_as_json(self) -> json:
        if self.format == 'json':
            return self._get_json_content()
        else:
            err = 'File has incorrect format. Json expected'
            raise ValueError(err)

    def _get_json_content(self) -> json:
        with open(self._path) as data:
            return json.load(data, encoding='UTF-8')

import glob


class Extractor(object):
    def __init__(self, path, fmt='json'):
        self._path = path
        self._format = fmt
        self._pattern = None

    def _get_pattern(self):
        """Pattern to match files in a given format"""
        return '/*'.join([self._path, self._format])

    def get_file_list(self):

        if not self._pattern:
            self._pattern = self._get_pattern()

        files = glob.glob(self._pattern)
        return [File(f) for f in files]


class File(object):
    def __init__(self, path):
        self._path = path

    def __str__(self):
        return self._path.split('/')[-1]

    def __repr__(self):
        return str(self)

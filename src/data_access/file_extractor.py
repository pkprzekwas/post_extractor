import glob

from data_access.file import File


class FileExtractor(object):
    """ Extracts files of given format from a directory. """
    def __init__(self, directory, fmt='json'):
        self._directory = directory
        self._format = fmt
        self._pattern = None

    def _get_pattern(self):
        """ Pattern to match files in a given format. """
        return '/*'.join([self._directory, self._format])

    def get_file_list(self):

        if not self._pattern:
            self._pattern = self._get_pattern()

        files = glob.glob(self._pattern)
        return [File(f) for f in files]

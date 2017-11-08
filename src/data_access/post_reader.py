from data_access.file_extractor import File


class PostReader(object):
    def __init__(self, file: File):
        self._file = file

    @property
    def posts(self):
        return self._read()

    def _read(self):
        """Return posts in a JSON format."""
        return self._file.content_as_json

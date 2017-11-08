from typing import List

from file_extractor import File
from post import Post


class PostReader(object):
    def __init__(self, file: File):
        self._file = file

    @property
    def posts(self) -> List[Post]:
        return self._read()

    def _read(self) -> List[Post]:
        """Return posts in a JSON format."""
        return self._file.content_as_json

from typing import List

from file_extractor import File
from post import Post


class PostReader(object):
    def __init__(self, file: File):
        self._file = file

    @property
    def posts(self) -> List[Post]:
        return self._read()

    def _read(self, n: int=None) -> List[Post]:
        """Return `n` posts in a list. All by default."""
        file_content = self._file.content_as_json
        posts = [
            Post(text=props.get('content')) for props in file_content
        ]
        return posts[:n]

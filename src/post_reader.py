from textblob import TextBlob
from textblob.exceptions import NotTranslated, TranslatorError

from file_extractor import File


class PostReader(object):
    def __init__(self, file: File):
        self._file = file

    @property
    def posts(self):
        return self._read(n=2)

    def _read(self, n: int=None) -> list:
        """Return `n` posts in a list. All by default."""
        file_content = self._file.content_as_json
        posts = [
            Post(text=props.get('content')) for props in file_content
        ]
        return posts[:n]


class Post(object):
    def __init__(self, text: str, author: str=None,
                 data: str=None, lang: str ='pl'):
        self.text = text
        self.author = author
        self.data = data
        self._lang = lang
        self._text_blob = TextBlob(self.text)

    def pl_to_en(self):
        try:
            return self._text_blob.translate(from_lang=self._lang)
        except (NotTranslated, TranslatorError) as err:
            print(err)

    @property
    def tags(self):
        return self._text_blob.tags

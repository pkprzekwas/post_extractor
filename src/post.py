from typing import List, Tuple

from textblob import TextBlob
from textblob.exceptions import NotTranslated, TranslatorError


class Post(object):
    def __init__(self, text: str, author: str=None, data: str=None):
        self.text = text
        self.author = author
        self.data = data
        self._text_blob = TextBlob(self.text)
	
    def __str__(self):
        return self.text

    def __repr__(self):
        return str(self)

    def translate(self, lang: str) -> str:
        try:
            return self._text_blob.translate(from_lang=lang)
        except (NotTranslated, TranslatorError) as err:
            print(err)

    @property
    def tags(self) -> List[Tuple]:
        return self._text_blob.tags

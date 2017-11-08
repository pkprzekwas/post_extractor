import operator
from collections import OrderedDict

from typing import List, Tuple

from post import Post


class SpeechPartsExtractor:
    def __init__(self, lang: str= 'eng'):
        super().__init__()
        self._lang = lang

    def run(self, posts):
        posts_content = [Post(text=props.get('content')) for props in posts]

        if self._lang != 'eng':
            translated_posts = self.to_eng(posts_content, self._lang)
            posts_content = translated_posts

        tags = []
        for content in posts_content:
            if content is not None:
                tags.extend(content.tags)

        stats = SpeechPartsExtractor.tags_stats(tags)

        return stats

    @staticmethod
    def to_eng(posts: list, lang: str) -> List[Post]:
        return [p.translate(lang=lang) for p in posts]

    @staticmethod
    def tags_stats(tags: List[Tuple]) -> OrderedDict:
        types = {}

        for tag in tags:
            if tag[1] in types:
                types[tag[1]] += 1
            else:
                types[tag[1]] = 1

        return OrderedDict(
            reversed(sorted(types.items(), key=operator.itemgetter(1)))
        )



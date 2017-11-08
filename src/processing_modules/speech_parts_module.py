import operator
from collections import OrderedDict

from processing_modules.base_module import BaseModule


class SpeechPartsModule(BaseModule):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def run(self, input_data):
        posts = input_data
        posts_content = self.get_content(posts)

        if self._lang != 'eng':
            translated_posts = self.to_eng(posts_content, self._lang)
            posts_content = translated_posts

        tags = []
        for content in posts_content:
            if content is not None:
                tags.extend(content.tags)

        speech_parts = self.tags_sum_by_key(tags)

        return speech_parts

    @staticmethod
    def tags_sum_by_key(tags):
        types = {}

        for tag in tags:
            if tag[1] in types:
                types[tag[1]] += 1
            else:
                types[tag[1]] = 1

        return OrderedDict(
            reversed(sorted(types.items(), key=operator.itemgetter(1)))
        )



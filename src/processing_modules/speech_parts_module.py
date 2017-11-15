import operator
from collections import OrderedDict

from processing_modules.base_module import BaseModule


class SpeechPartsModule(BaseModule):
    def __init__(self):
        super().__init__()

    def run(self, input_data):
        posts = input_data.get('posts')

        if input_data.get('translated') is False:
            posts = self.translate(posts)
            input_data.update({'posts': posts, 'translated': True})

        tags = []
        for content in posts:
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



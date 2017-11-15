from processing_modules.base_module import BaseModule


class SentenceModule(BaseModule):

    def run(self, input_data):
        posts = input_data.get('posts')

        if input_data.get('translated') is False:
            posts = self.translate(posts)
            input_data.update({'posts': posts, 'translated': True})

        sentences = []
        for content in posts:
            if content is not None:
                sentences.extend(content.sentences)

        return sentences

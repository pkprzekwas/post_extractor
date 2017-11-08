from processing_modules.base_module import BaseModule


class SentenceModule(BaseModule):
    def run(self, input_data):
        posts = input_data
        posts_content = self.get_content(posts)

        if self._lang != 'eng':
            translated_posts = self.to_eng(posts_content, self._lang)
            posts_content = translated_posts

        sentences = []
        for content in posts_content:
            if content is not None:
                sentences.extend(content.sentences)

        return sentences

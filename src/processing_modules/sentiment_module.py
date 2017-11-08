from processing_modules.base_module import BaseModule


class SentimentModule(BaseModule):
    def run(self, input_data):
        posts = input_data
        posts_content = self.get_content(posts)

        if self._lang != 'eng':
            translated_posts = self.to_eng(posts_content, self._lang)
            posts_content = translated_posts

        sentiments = []
        for content in posts_content:
            if content is not None:
                sentiments.extend(content.sentiment)

        return sentiments

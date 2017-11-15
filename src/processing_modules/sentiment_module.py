from processing_modules.base_module import BaseModule


class SentimentModule(BaseModule):
    def run(self, input_data):
        posts = input_data.get('posts')

        if input_data.get('translated') is False:
            posts = self.translate(posts)
            input_data.update({'posts': posts, 'translated': True})

        sentiments = []
        for content in posts:
            if content is not None:
                sentiments.extend(content.sentiment)

        return sentiments

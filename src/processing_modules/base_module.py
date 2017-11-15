import abc

from data_access.post import Post


class BaseModule(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def run(self, input_data):
        """Main runner for module."""

    @staticmethod
    def translate(posts):
        posts_content = BaseModule.get_content(posts)
        # translated_posts = BaseModule.to_eng(posts_content)
        return posts_content
        # return translated_posts

    @staticmethod
    def to_eng(posts, lang='pl'):
        return [p.translate(lang=lang) for p in posts]

    @staticmethod
    def get_content(posts):
        return [Post(text=props.get('content')) for props in posts]

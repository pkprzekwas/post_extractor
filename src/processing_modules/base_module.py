import abc

from data_access.post import Post


class BaseModule(metaclass=abc.ABCMeta):

    def __init__(self, lang=None):
        self._lang = lang

    @abc.abstractmethod
    def run(self, input_data):
        """Main runner for module."""

    @staticmethod
    def to_eng(posts, lang):
        return [p.translate(lang=lang) for p in posts]

    @staticmethod
    def get_content(posts):
        return [Post(text=props.get('content')) for props in posts]

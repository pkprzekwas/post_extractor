import json


class File(object):
    def __init__(self, path: str):
        self._path = path
        self._content = None
        self._fmt = None

    def __str__(self):
        return self._path.split('/')[-1]

    def __repr__(self):
        return str(self)

    def _fmt_from_path(self):
        return self._path.split('.')[-1].lower()

    @property
    def format(self):
        return self._fmt_from_path()

    @property
    def content_as_json(self):
        if self.format == 'json':
            return self._get_json_content()
        else:
            err = 'File has incorrect format. Json expected'
            raise ValueError(err)

    def _get_json_content(self):
        with open(self._path) as data:
            return json.load(data, encoding='UTF-8')

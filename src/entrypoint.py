import conf
from data_access.post_reader import PostReader
from data_access.storage import DataStorageService
from process import process


def get_posts(file):
    """Return posts in JSON format"""
    reader = PostReader(file)
    p = reader.posts
    return p


def extract_posts_from_files(file_list):
    """Return list of JSON posts"""
    posts_as_json = []
    for file in file_list:
        if file is not None:
            posts_as_json.extend(get_posts(file))
    return posts_as_json


if __name__ == '__main__':

    configuration = conf.CONFIG
    storage_service = DataStorageService(configuration)

    files = storage_service.read_all()
    posts = extract_posts_from_files(files)

    data = {
        'posts': posts,
        'translated': False
    }

    results = process(data)

    storage_service.save_all(feature_json=results)

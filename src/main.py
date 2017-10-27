from src.extractor import Extractor
from conf import DATA_PATH


if __name__ == '__main__':

    e = Extractor(path=DATA_PATH)
    files = e.get_file_list()

    for f in files:
        print(f)

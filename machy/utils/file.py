from os import path, makedirs


def create_file_if_does_not_exist(filename):
    """
    Create file if it does not exist
    :param filename: filename
    """
    dirname = path.dirname(filename)
    makedirs(dirname, exist_ok=True)
    open(filename, 'a+')

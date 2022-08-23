import functools


class LogDecorator(object):
    def __call__(self, fn):
        @functools.wraps(fn)
        def decorated(*args, **kwargs):
            try:
                print("[log-decorator] {0} - {1} - {2}".format(fn.__name__, args, kwargs))
            except Exception as ex:
                print("Exception {0}".format(ex))
                raise ex

            return fn(*args, **kwargs)

        return decorated


def log(*strings):
    print()
    for string in strings:
        print(string)

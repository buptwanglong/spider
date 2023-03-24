class T(object):
    def __init__(self, a=1):
        self.a = a
        self.func = None

    def __call__(self, func):
        self.func = func
        print('call')

        def inner(*args, **kwargs):
            return func(*args, **kwargs)

        return inner


@T()
def d(p):
    print(p)


if __name__ == '__main__':
    print('start')
    # d(1)

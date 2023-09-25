if __name__ == '__main__':
    import importlib

    e = importlib.import_module("task_entry")
    d = importlib.import_module('dd')

    print(e.__dict__)
    print(d.__dict__)
    print(e)

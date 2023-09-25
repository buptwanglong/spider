# 全局ctx
from gevent.lock import RLock


class GContext(object):
    def __init__(self):
        self.lock = RLock()

    def get(self, name, default=None):
        with self.lock:
            return getattr(self, name, default)

    def add(self, name, value):
        with self.lock:
            if hasattr(self, name):
                raise Exception('ctx has name, you can use update')
            setattr(self, name, value)

    def update(self, name, value):
        with self.lock:
            if not hasattr(self, name):
                raise Exception('ctx not has name, you can use add')

            setattr(self, name, value)

    def add_or_update(self, name, value):
        with self.lock:
            setattr(self, name, value)


g_ctx = GContext()

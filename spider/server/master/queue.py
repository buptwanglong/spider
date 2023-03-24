import gevent
from gevent.queue import PriorityQueue
from gevent.lock import RLock


class ReadyTaskPQ(object):
    def __init__(self, q: PriorityQueue, name):
        self.q = q
        self.name = name
        self.lock = RLock()
        self.snapshorts = []

    def pop(self):
        with self.lock:
            score, item = self.q.get()
            return score, item

    def put(self, score, item):
        with self.lock:
            self.q.put((score, item))

    def size(self):
        with self.lock:
            return self.q.qsize()

    def get_snapshorts(self):
        return self.snapshorts

    def snapshot_down(self):
        while True:
            self.snapshorts.append(self.q.qsize())
            if len(self.snapshorts) > 1000:
                self.snapshorts = self.snapshorts[-500:]


DEFAULT_READY_TASK_PQ = ReadyTaskPQ(q=PriorityQueue(), name='default')

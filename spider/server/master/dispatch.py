import gevent

from spider.server.master.que import ReadyTaskPQ, DEFAULT_READY_TASK_PQ
from enum import Enum
from spider.server.worker.worker import WorkState
import random
from spider.backend import BaseBackend, load_backend
from spider.server.models.sql_alchemy import WorkerMysqlModel, TaskMysqlModel
from typing import List, Generator
from spider.protocol import Message
from typing import Optional
from gevent.queue import Empty
from util.log import logger


class StrategyTypeEnum(str, Enum):
    RANDOM = "Random"
    PERFORMANCE = "Performance"


class Strategy(object):
    def __init__(self, backend: BaseBackend, ste=StrategyTypeEnum.RANDOM):
        self.bk = backend
        self.ste = ste

    def random_st(self) -> Optional[str]:
        w_l = [
            worker_id for worker_id in self.bk.worker_state_get(state=WorkState.RUNNING)
        ]
        if w_l:
            return random.choice(w_l)
        else:
            return None

    def performance_st(self):
        raise NotImplementedError

    def choose_worker(self):
        if self.ste == StrategyTypeEnum.RANDOM:
            return self.random_st()
        else:
            raise NotImplementedError
        pass


class TaskDispatcher(object):
    def __init__(self, q: ReadyTaskPQ = None, backend: BaseBackend = None):
        self.q = q or DEFAULT_READY_TASK_PQ
        self.bk = backend or load_backend()
        self.strategy = Strategy(backend=backend)

    def dispatch(self) -> Generator:
        while True:
            try:
                task: TaskMysqlModel = self.q.pop(timeout=10)
            except Empty as _:
                logger.info("queue is empty,%s,and sleep 10s", self.q.name)
                gevent.sleep(5)
            else:
                worker_id = self.strategy.choose_worker()
                if worker_id:
                    msg: Message = task.to_msg()
                    msg.worker_id = worker_id
                    yield msg


if __name__ == '__main__':
    td = TaskDispatcher()
    for t in td.dispatch():
        print(t)

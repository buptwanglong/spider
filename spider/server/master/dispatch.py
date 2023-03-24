import gevent

from spider.server.master.queue import ReadyTaskPQ
from enum import Enum
from spider.server.worker.worker import WorkState
import random
from spider.backend import BaseBackend
from spider.server.models.sql_alchemy import WorkerMysqlModel, TaskMysqlModel
from typing import List, Generator
from spider.protocol import Message
from typing import Optional


class StrategyTypeEnum(str, Enum):
    RANDOM = "Random"
    PERFORMANCE = "Performance"


class Strategy(object):
    def __init__(self, backend: BaseBackend, ste=StrategyTypeEnum.RANDOM):
        self.bk = backend
        self.ste = ste

    def random_st(self) -> Optional[str]:
        w_l = [
            worker_id for worker_id in self.bk.worker_state_get(state=WorkState.RUNNING) if
            self.bk.get_worker_state()[worker_id] == WorkState.RUNNING
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
    def __init__(self, q: ReadyTaskPQ, backend: BaseBackend):
        self.q = q
        self.bk = backend
        self.strategy = Strategy(backend=backend)

    def dispatch(self) -> Generator:
        while True:
            task: TaskMysqlModel = self.q.pop()
            worker_id = self.strategy.choose_worker()
            if worker_id:
                msg: Message = task.to_msg()
                msg.worker_id = worker_id
                yield msg

            else:
                gevent.sleep(10)

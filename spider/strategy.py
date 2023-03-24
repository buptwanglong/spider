import random

from spider.backend import BaseBackend
from enum import Enum
from spider.server.worker import WorkState


class StrategyTypeEnum(str, Enum):
    RANDOM = "Random"
    PERFORMANCE = "Performance"


class Strategy(object):
    def __init__(self, backend: BaseBackend, ste=StrategyTypeEnum.RANDOM):
        self.bk = backend
        self.ste = ste

    def random_st(self) -> str:
        w_l = [
            worker_id for worker_id in self.bk.get_worker_state() if
            self.bk.get_worker_state()[worker_id] == WorkState.RUNNING
        ]
        return random.choice(w_l)

    def performance_st(self):
        raise NotImplementedError

    def choose_worker(self):
        if self.ste == StrategyTypeEnum.RANDOM:
            return self.random_st()
        else:
            raise NotImplementedError
        pass

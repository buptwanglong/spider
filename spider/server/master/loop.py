from spider.backend import BaseBackend
from spider.cron import Cron
from typing import Optional

from spider.server.master.queue import DEFAULT_READY_TASK_PQ, ReadyTaskPQ


class ReadyTaskLoop(object):
    def __init__(self, backend: BaseBackend, queue: Optional[ReadyTaskPQ] = None):
        self.bk = backend
        self.queue: ReadyTaskPQ = queue or DEFAULT_READY_TASK_PQ

    def loop(self):
        while True:
            tasks = self.bk.get_ready_tasks()
            for task in tasks or []:
                last_run_ts = Cron(task.cron).last_run
                self.queue.put(last_run_ts, task)

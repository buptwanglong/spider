import gevent

from spider.backend import BaseBackend
from spider.cron import Cron
from typing import Optional

from spider.server.master.que import DEFAULT_READY_TASK_PQ, ReadyTaskPQ
from spider.util.log import logger
from sqlalchemy.orm.query import Query


class ReadyTaskLoop(object):
    def __init__(self, backend: BaseBackend, queue: Optional[ReadyTaskPQ] = None):
        self.bk = backend
        self.queue: ReadyTaskPQ = queue or DEFAULT_READY_TASK_PQ

    def loop(self):
        print("spawn loop")
        while True:
            try:
                tasks = self.bk.get_ready_tasks()
                print('tasks', tasks, type(tasks))
                if tasks:
                    for task in tasks:
                        last_run_ts = Cron(task.cron).last_run
                        self.queue.put(last_run_ts, task)
                else:
                    print("bk has no ready tasks")
                    gevent.sleep(1)
                    print("bk has no ready tasks")
                    logger.info("bk has no ready tasks")
            except Exception as e:
                print('ex', e)


if __name__ == '__main__':
    from spider.server.config import load_conf, ServerConf
    from spider.backend import load_backend

    conf: ServerConf = load_conf("/Users/wanglong/projects/spider/examples/example_server_a/conf.yaml")
    bk = load_backend(conf.master2backend)
    rl = ReadyTaskLoop(bk)
    rl.loop()

    pass

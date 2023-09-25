import signal

import gevent
from gevent import Greenlet

from spider.util.memcache import cached_property
from spider.util.zmqrpc import Server
from spider.protocol import Message, MessageTypeEnum
from spider import backend
from spider.server.master.que import DEFAULT_READY_TASK_PQ
from spider.server.master.loop import ReadyTaskLoop
from spider.server.master.dispatch import TaskDispatcher
from enum import Enum
from spider.server.config import ServerConf
from spider.server import config
from spider.util.common import local_cli_id
from spider.util.log import logger
from typing import Dict


class MasterState(str, Enum):
    RUNNING = 'running'
    DEAD = 'dead'
    READY = 'ready'


class Master(object):
    def __init__(self, conf_path=None):
        self._conf_path = conf_path
        # task queue
        self.q = DEFAULT_READY_TASK_PQ
        # loop and put q into q
        self.loop = ReadyTaskLoop(backend=self.backend, queue=self.q)
        # get task from q and dispatcher
        self.dispatcher = TaskDispatcher(backend=self.backend, q=self.q)

        self.rpcserver = Server(host=self.host, port=self.port)
        self.state = MasterState.READY

        signal.signal(signal.SIGINT, self.terminate)

        self.g_map: Dict[str, Greenlet] = dict()

    @cached_property
    def conf(self) -> ServerConf:
        return config.load_conf(conf_path=self._conf_path)

    @property
    def host(self):
        return self.conf.master2host

    @property
    def port(self):
        return self.conf.master2port

    @cached_property
    def backend(self) -> backend.BaseBackend:
        return backend.load_backend(self.conf.master2backend)

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, state):
        self._state = state

    @cached_property
    def id(self):
        return local_cli_id()

    def msg_handler(self, msg: Message):
        if msg.m_type == MessageTypeEnum.HEART_BEAT:
            self.backend.heart_info_add(msg)
        elif msg.m_type == MessageTypeEnum.TASK_LOG:
            self.backend.task_log_add(msg)
        elif msg.m_type == MessageTypeEnum.WORKER:
            self.backend.work_state_add(msg)

    def rpc_serve(self):
        while self.state != MasterState.DEAD:
            try:
                node_id, _msg = self.rpcserver.recv_from_client()
                msg: Message = _msg
                logger.debug("accept msg %s" % msg)
                self.msg_handler(msg)
            except Exception as e:
                logger.info("no data,wait", e)
                gevent.sleep(1)

    def dispatch_and_send(self):
        print("spawn dispatch")
        for msg in self.dispatcher.dispatch():
            self.rpcserver.send2client(msg)

    def entry(self):
        self.g_map['loop_g'] = gevent.spawn(self.loop.loop)
        self.g_map['dispatch_g'] = gevent.spawn(self.dispatch_and_send)
        self.g_map['q_snapshot_down_g'] = gevent.spawn(self.q.snapshot_down)
        self.g_map['rpc_server_g'] = gevent.spawn(self.rpc_serve)
        gevent.joinall(self.g_map.values())

    def terminate(self, signum, frame, *args, **kwargs):
        logger.info(f"terminate the task gen loop")
        self.g_map['loop_g'].kill()
        logger.info(f"terminate the snapshot down g")
        self.g_map['q_snapshot_down_g'].kill()
        while self.q.size() != 0:
            logger.warning(f"has {self.q.size()} tasks in q:{self.q.name} do not send to worker ")

        logger.info(f"tasks empty in q {self.q.name}")
        logger.info(f"terminate the dispatch_g")
        self.g_map['dispatch_g'].kill()
        logger.info(f"set state to dead")
        self.state = MasterState.DEAD


if __name__ == '__main__':
    pass

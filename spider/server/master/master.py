import datetime
import logging
import os
import queue
import signal
import urllib.parse

import gevent

from spider.util.memcache import cached_property
from spider.util.zmqrpc import Server
from spider.protocol import Message, MessageTypeEnum
from spider.backend import SqlAlchemyBackend, BaseBackend
from spider.server.master.queue import DEFAULT_READY_TASK_PQ
from spider.server.master.loop import ReadyTaskLoop
from spider.server.master.dispatch import TaskDispatcher
from enum import Enum
import yaml

logger = logging.Logger(__file__)


class MasterState(str, Enum):
    RUNNING = 'running'
    DEAD = 'dead'
    READY = 'ready'


class Master(object):
    def __init__(self, conf_path=None):
        self._conf_path = conf_path

        self.ready_queue = queue.Queue()
        # task queue
        self.q = DEFAULT_READY_TASK_PQ
        # loop and put q into q
        self.loop = ReadyTaskLoop(backend=self.backend, queue=self.q)
        # get task from q and dispatcher
        self.dispatcher = TaskDispatcher(backend=self.backend, q=self.q)

        self.rpcserver = Server(host=self.host, port=self.port)
        self.state = MasterState.READY

        signal.signal(signal.SIGTERM, self.terminate)

        self.g_map = dict()

    @cached_property
    def conf(self) -> dict:
        if os.path.abspath(self._conf_path):
            _conf = yaml.safe_load(open(self._conf_path))
        else:
            _conf = yaml.safe_load(open(os.path.join(os.getcwd(), self._conf_path)))
        return _conf

    @property
    def host(self):
        _host = self.conf.get("master", {}).get("host")
        if not _host:
            raise Exception("Conf Error: master host should not be empty")

        return _host

    @property
    def port(self):
        _port = self.conf.get("master", {}).get("port")
        if not _port:
            raise Exception("Conf Error: master port should not be empty")
        return int(_port)

    @cached_property
    def backend(self) -> BaseBackend:
        _backend = self.conf.get("master", {}).get("backend")
        if _backend:
            _bk_url = urllib.parse.urlparse(_backend)
            if _bk_url.scheme in ['sqlite', 'mysql']:
                return SqlAlchemyBackend(_backend)
            else:
                raise Exception("Now only support sqlite and mysql")
        else:
            return SqlAlchemyBackend()

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, state):
        self._state = state

    @cached_property
    def id(self):
        return f"{self.host}::{self.port}::{os.getpid()}"

    def msg_handler(self, msg: Message):
        if msg.m_type == MessageTypeEnum.HEART_BEAT:
            self.backend.heart_info_add(msg)
        elif msg.m_type == MessageTypeEnum.TASK_LOG:
            self.backend.task_log_add(msg)
        elif msg.m_type == MessageTypeEnum.WORKER:
            self.backend.work_state_add(msg)

    def rpc_serve(self):
        while self.state != MasterState.DEAD:
            node_id, _msg = self.rpcserver.recv_from_client()
            msg: Message = _msg
            self.msg_handler(msg)

    def dispatch_and_send(self):
        for msg in self.dispatcher.dispatch():
            self.rpcserver.send2client(msg)

    def entry(self):
        self.g_map['loop_g'] = gevent.spawn(self.loop.loop)
        self.g_map['dispatch_g'] = gevent.spawn(self.dispatch_and_send)
        self.g_map['q_snapshot_down_g'] = gevent.spawn(self.q.snapshot_down)
        self.rpc_serve()

    def terminate(self):
        logger.info(f"terminate the task gen loop")
        self.g_map['loop_g'].kill()
        logger.info(f"terminate the snapshot down g")
        self.g_map['q_snapshot_down_g'].kill()
        while self.q.size() != 0:
            logger.warning(f"has {self.q.size()} tasks in q:{self.q.name} do not send to worker ")

        logger.info(f"tasks empty in q {self.q.name}")
        logger.info(f"terminate the dispatch_g")
        self.g_map['dispatch_g'].kill()

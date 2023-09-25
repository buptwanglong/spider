import datetime
import signal
from spider.util.memcache import cached_property
from spider.util.zmqrpc import Client
from enum import Enum
from spider.protocol import Message, MessageTypeEnum, Task, HeartInfo, Worker
import psutil
import gevent
import subprocess
from spider.server.worker.processor import WorkerProcessor
from typing import List
from spider.server import config
from spider.util.common import local_cli_id
from spider.util.log import logger


class WorkState(str, Enum):
    READY = 'Ready'
    RUNNING = 'Running'
    DEAD = 'Dead'


class WorkerGroup(object):
    def __init__(self, conf_path):
        self.conf_path = conf_path
        self.id = local_cli_id()
        self._state = WorkState.READY
        self._workers: List[gevent.Greenlet] = []
        signal.signal(signal.SIGTERM, self.warm_terminate)

    @cached_property
    def conf(self) -> config.ServerConf:
        conf = config.load_conf(self.conf_path)
        return conf

    @cached_property
    def client(self) -> Client:
        print('client', self.master_host, self.master_port, self.id)
        return Client(host=self.master_host, port=self.master_port, identity=self.id)

    @cached_property
    def master_host(self):
        return self.conf.master2host

    @cached_property
    def master_port(self) -> str:
        return self.conf.master2port

    @cached_property
    def work_dir(self):
        return self.conf.woker2work_dir

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, state: WorkState):
        if state != self._state:
            self._state = state
            self.client.send(Message(
                m_type=MessageTypeEnum.WORKER,
                data=Worker(id=self.id, state=self._state),
                client_id=self.id,
                timestamp=int(datetime.datetime.now().timestamp())
            ))

    @property
    def cpu_percent(self) -> float:
        return psutil.cpu_percent()

    @property
    def workers(self):
        rst = [w for w in self._workers if not w.dead()]
        self._workers = rst
        return self._workers

    @property
    def mem_percent(self) -> float:
        vm = psutil.virtual_memory()
        return vm.percent

    def heart_beat(self, interval=1):
        while self.state != WorkState.DEAD:
            try:
                logger.debug("msg send")
                self.client.send(Message(
                    m_type=MessageTypeEnum.HEART_BEAT,
                    data=HeartInfo(
                        cpu_percent=self.cpu_percent,
                        mem_percent=self.mem_percent
                    ),
                    client_id=self.id,
                    timestamp=int(datetime.datetime.now().timestamp())
                ))
                gevent.sleep(interval)
            except Exception as e:
                logger.error("heart beat err %s" % e)

    def msg_handler(self):
        while self.state != WorkState.DEAD:
            msg: Message = self.client.recv()
            if msg.m_type == MessageTypeEnum.TASK:
                self._workers.append(gevent.spawn(WorkerProcessor(task=Task(**msg.data), wg=self).core))

    def worker_server(self):
        self.env_check()
        # 心跳机制
        self._workers.append(gevent.spawn(self.heart_beat))
        # 消息处理
        self._workers.append(gevent.spawn(self.msg_handler))
        gevent.joinall(self.workers)

    def env_check(self):
        try:
            subprocess.check_call(["docker", "-v"])
        except Exception as ex:
            print(ex)
            raise Exception("docker should be installed in the env")

    def warm_terminate(self):
        self.state = WorkState.DEAD
        while len(self.workers) != 0:
            print("have workers process task, please wait")

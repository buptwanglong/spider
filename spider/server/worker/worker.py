import datetime
import os
import signal
import yaml
from spider.util.memcache import cached_property
from spider.util.zmqrpc import Client
from enum import Enum
from spider.protocol import Message, MessageTypeEnum, Task, HeartInfo, TaskLog, TaskStatus, Worker
import psutil
import gevent
import subprocess
from spider.server.worker.processor import WorkerProcessor
from typing import List
from spider.server.config import SERVER_CONF, ServerConf


class WorkState(str, Enum):
    READY = 'Ready'
    RUNNING = 'Running'
    DEAD = 'dead'


class WorkerGroup(object):
    def __init__(self):
        self.id = f"{SERVER_CONF.master2host}::{SERVER_CONF.master2port}::{os.getpid()}"
        self._state = WorkState.READY
        self._workers: List[gevent.Greenlet] = []
        signal.signal(signal.SIGTERM, self.warm_terminate)

    @cached_property
    def client(self) -> Client:
        return Client(host=self.master_host, port=self.master_port, identity=self.id)

    @cached_property
    def master_host(self):

        _host = self.conf.master2host
        return _host

    @cached_property
    def master_port(self):
        _port = self.conf.get("worker", {}).get('master_port')
        if not _port:
            raise Exception("worker's port host should not be empty")

    @cached_property
    def work_dir(self):
        _work_dir = self.conf.get("worker", {}).get("work_dir")
        if not _work_dir:
            raise Exception("worker' worker dir should not be empty")

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

    def heart_beat(self):
        while self.state != WorkState.DEAD:
            self.client.send(Message(
                m_type=MessageTypeEnum.HEART_BEAT,
                data=HeartInfo(
                    cpu_percent=self.cpu_percent,
                    mem_percent=self.mem_percent
                ),
                client_id=self.id,
                timestamp=int(datetime.datetime.now().timestamp())
            ))

    def worker_server(self):
        self.env_check()

        def task_state_listener(task_log: TaskLog):
            self.client.send(Message(
                m_type=MessageTypeEnum.TASK_LOG,
                data=task_log,
                client_id=self.id,
                timestamp=int(datetime.datetime.now().timestamp())
            ))

        while self.state != WorkState.DEAD:
            msg: Message = self.client.recv()
            if msg.m_type == MessageTypeEnum.TASK:
                task = Task(**msg.data)
                task_log = TaskLog(name=task.name, task_set=task.task_set, state=TaskStatus.READY, reason='',
                                   logid='',
                                   state_listener=task_state_listener)
                task_log.state = TaskStatus.READY  # trigger hooks
                g: gevent.Greenlet = gevent.spawn(
                    WorkerProcessor(task=task, worker_path=self.work_dir, task_log=task_log).core)

                self._workers.append(g)

    def env_check(self):
        try:
            subprocess.check_call("docker -v")
        except Exception as ex:
            raise Exception("docker should be installed in the env")

    def warm_terminate(self):
        self.state = WorkState.DEAD
        while len(self.workers) != 0:
            print("have workers process task, please wait")

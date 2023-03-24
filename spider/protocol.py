import json
from enum import Enum
from typing import TypeVar
import msgpack
from dataclasses import dataclass
from typing import Callable


class MessageTypeEnum(str, Enum):
    HEART_BEAT = 'heart_beat'
    TASK = 'task'
    TASK_LOG = 'task_log'
    WORKER = 'worker'


class TaskStatus(str, Enum):
    READY = 'ready'
    RUNNING = 'running'
    FAILED = 'failed'
    SUCCESS = 'success'
    pass


JSON = TypeVar("JSON", dict, list)


class DictObj(dict):
    def __init__(self, *args, **kwargs):
        super(DictObj, self).__init__(*args, **kwargs)

    def __getattr__(self, item):
        try:
            return self[item]
        except KeyError:
            raise AttributeError(f"{self.__class__.__name__} has no attribute {item}")

    def __setattr__(self, key, value):
        self[key] = value


class Message(DictObj):
    def __init__(self, m_type: MessageTypeEnum, data: DictObj, client_id, timestamp: int):
        self.m_type = m_type
        self.data = data
        self.timestamp = timestamp
        self.worker_id = client_id
        super(Message, self).__init__()

    def serialize(self):
        return json.dumps(self)

    @classmethod
    def unserialize(cls, msg_data):
        return json.loads(msg_data)


class Task(DictObj):
    def __init__(self, name: str,
                 repo: dict,
                 task_conf: dict,
                 task_set: str,
                 cron: str):
        self.name = name
        self.repo = repo
        self.task_conf = task_conf
        self.task_set = task_set
        self.cron = cron
        super(Task, self).__init__()


class HeartInfo(DictObj):
    def __init__(self, cpu_percent, mem_percent):
        self.cpu_percent = cpu_percent
        self.mem_percent = mem_percent
        super(HeartInfo, self).__init__()


class TaskLog(DictObj):
    def __init__(self, name, task_set, state, reason, logid, state_listener: Callable):
        self.name = name
        self.task_set = task_set
        self._state = state
        self.reason = reason
        self.logid = logid
        self.state_listener = state_listener
        super(TaskLog, self).__init__()

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, state):
        if state != self._state:
            self._state = state
            self.state_listener(self)


class Worker(DictObj):
    def __init__(self, id, state):
        self.id = id
        self.state = state
        super(Worker, self).__init__()

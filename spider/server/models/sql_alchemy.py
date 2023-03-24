import datetime
from sqlalchemy import String, JSON, TIMESTAMP, Column, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session
from sqlalchemy import FLOAT
from spider.protocol import Message, HeartInfo, MessageTypeEnum, Task, TaskLog, Worker

import contextlib

Base = declarative_base()


class TaskMysqlModel(Base):
    __tablename__ = 'task'
    id = Column('id', Integer, autoincrement=True, primary_key=True)
    name = Column('name', String)
    repo = Column('repo', JSON)
    task_conf = Column('task_conf', JSON)
    task_set = Column('task_set', JSON)
    cron = Column('cron', String)
    next_run_at = Column('next_run_at', TIMESTAMP)

    @classmethod
    def from_vo(cls, task: Task):
        return cls(name=task.name, repo=task.repo, task_conf=task.task_conf, task_set=task.task_set)

    @classmethod
    def from_msg(cls, msg: Message):
        data: Task = msg.data
        return cls(
            name=data.name,
            repo=data.repo,
            task_conf=data.task_conf,
            task_set=data.task_set,
            cron=data.cron
        )

    def to_msg(self):
        return Message(
            m_type=MessageTypeEnum.TASK,
            data=Task(
                name=self.name,
                repo=self.repo,
                task_conf=self.task_conf,
                task_set=self.task_set,
                cron=self.cron
            ),
            timestamp=int(datetime.datetime.now().timestamp()),
            client_id=None
        )


class HeartInfoMysqlModel(Base):
    __tablename__ = 'heart_info'
    id = Column('id', Integer, autoincrement=True, primary_key=True)
    cpu_percent = Column('cpu_percent', FLOAT)
    mem_percent = Column('mem_percent', FLOAT)
    node_id = Column('node_id', String)
    timestamp = Column('timestamp', TIMESTAMP())

    @classmethod
    def from_message(cls, msg: Message):
        data: HeartInfo = msg.data
        return cls(
            cpu_percent=data.cpu_percent,
            mem_percent=data.mem_percent,
            node_id=msg.worker_id,
            timestamp=msg.timestamp
        )


class TaskLogMysqlModel(Base):
    __tablename__ = 'task_log'
    id = Column('id', Integer, autoincrement=True, primary_key=True)
    name = Column(String)
    task_set = Column(String)
    state = Column(String)
    reason = Column(String)
    logid = Column(String)

    @classmethod
    def from_msg(cls, msg: Message):
        data: TaskLog = msg.data
        return cls(
            name=data.name,
            task_set=data.task_set,
            state=data.state,
            reason=data.reason,
            logid=data.logid
        )


class WorkerMysqlModel(Base):
    __tablename__ = 'worker'
    id = Column('id', Integer, autoincrement=True, primary_key=True)
    worker_id = Column('worker_id', String)
    state = Column('state', String)

    @classmethod
    def from_msg(cls, msg: Message):
        data: Worker = msg.data
        return cls(
            id=data.id,
            state=data.state
        )

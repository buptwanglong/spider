import datetime

from spider.protocol import Message, MessageTypeEnum
from abc import ABCMeta, abstractmethod
from spider.server.models.sql_alchemy import HeartInfoMysqlModel, TaskMysqlModel, WorkerMysqlModel, \
    Base
from sqlalchemy.sql import or_

from typing import List, Optional

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.orm.query import Query

import contextlib


class BaseBackend(metaclass=ABCMeta):

    @abstractmethod
    def with_session(self):
        ...

    @abstractmethod
    def heart_info_add(self, msg: Message):
        ...

    @abstractmethod
    def task_add(self, msg: Message):
        ...

    @abstractmethod
    def task_log_add(self, msg: Message):
        ...

    @abstractmethod
    def work_state_add(self, msg: Message):
        ...

    @abstractmethod
    def worker_state_get(self, state):
        ...

    @abstractmethod
    def get_ready_tasks(self) -> List[TaskMysqlModel]:
        ...

    @abstractmethod
    def tasks_bulk_save(self, tasks: TaskMysqlModel):
        ...

    @abstractmethod
    def meta_init(self):
        ...


class SqlAlchemyBackend(BaseBackend):
    def __init__(self, bk_url="sqlite:///spider.db"):
        self.bk_url = bk_url or "sqlite:///spider.db"
        self.engin = create_engine(url=bk_url, pool_size=10, max_overflow=20)
        self.session_maker = sessionmaker(bind=self.engin, expire_on_commit=False)

    @contextlib.contextmanager
    def with_session(self) -> Session:
        session = self.session_maker()
        try:
            # this is where the "work" happens!
            yield session
            # always commit changes!
            session.commit()
        except Exception:
            # if any kind of exception occurs, rollback transaction
            session.rollback()
            raise
        finally:
            session.close()

    def heart_info_add(self, msg: Message):
        if msg.m_type != MessageTypeEnum.HEART_BEAT:
            raise Exception("error msg type in bk save")
        with self.with_session() as s:
            s.add(HeartInfoMysqlModel.from_message(msg))

    def task_add(self, msg: Message):
        if msg.m_type != MessageTypeEnum.TASK:
            raise Exception("error msg type in bk save")
        with self.with_session() as s:
            s.add(TaskMysqlModel.from_msg(msg))



    def task_log_add(self, msg: Message):
        if msg.m_type != MessageTypeEnum.TASK_LOG:
            raise Exception("error msg type in bk save")

        with self.with_session() as s:
            s.add(TaskMysqlModel.from_msg(msg))

    def work_state_add(self, msg: Message):
        if msg.m_type != MessageTypeEnum.WORKER:
            raise Exception("error msg type in bk save")

        with self.with_session() as s:
            s.add(WorkerMysqlModel.from_msg(msg))

    def tasks_bulk_save(self, tasks: TaskMysqlModel):
        with self.with_session() as s:
            s.bulk_save_objects(tasks)

    def get_ready_tasks(self):
        with self.with_session() as s:
            qs: Query = s.query(TaskMysqlModel).filter(
                or_(TaskMysqlModel.next_run_at == None, TaskMysqlModel.next_run_at < datetime.datetime.now()))
            print(qs)
            data = qs.all()
            print(len(data))
            return data

    def worker_state_get(self, state):
        with self.with_session() as s:
            qs = s.query(WorkerMysqlModel).filter(WorkerMysqlModel.state == state)
            return qs

    def meta_init(self):
        Base.metadata.create_all(self.engin)  # 创建表结构


def load_backend(bk_url='sqlite:////Users/wanglong/projects/spider/examples/example_server_a/spider.db'):
    bk = SqlAlchemyBackend(bk_url=bk_url)
    return bk


if __name__ == '__main__':
    bk = load_backend()
    for item in bk.get_ready_tasks():
        print(item)

    # sb.meta_init()

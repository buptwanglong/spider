from typing import Callable

from enum import Enum
from typing import Optional, List
from dataclasses import dataclass
from spider.cron import Cron


class TaskStatusEnum(str, Enum):
    READY = 'Ready'
    RUNNING = 'Running'
    FAILED = 'Failed'
    SUCCESS = 'Success'


class TaskHookEnum(str, Enum):
    ON_SUCCESS = 'OnSuccess'
    ON_FINALLY = 'OnFinally'
    ON_FAILED = 'OnFailed'
    ON_READY = 'OnReady'


@dataclass
class TaskHook(object):
    hook_name: str
    hook_call: Callable


class Task(object):

    def __init__(self, retry=1,
                 name=None,
                 hooks: Optional[List[TaskHook]] = None,
                 cron: Optional[str] = None,
                 func: Optional[Callable] = None,
                 *args, **kwargs):
        self.retry = retry
        self.hook = {h.hook_name: h.hook_call for h in hooks or []}
        self.func: Optional[Callable] = func
        self.name = name
        if cron and type(cron) == str:
            self.cron = Cron(crontab=cron)

        self.func_args = args,
        self.func_kw = kwargs

    @property
    def status(self):
        return TaskStatusEnum.READY

    @status.setter
    def status(self, sts):
        self.status = sts

    def success_hook(self):
        try:
            h = self.hook.get(TaskHookEnum.ON_SUCCESS)
            h()
        except Exception as ex:
            pass
        finally:
            self.status = TaskStatusEnum.SUCCESS

    def ready_hook(self):
        try:
            h = self.hook.get(TaskHookEnum.ON_READY)
            h()
        except Exception as ex:
            pass
        finally:
            self.status = TaskStatusEnum.READY

    def failed_hook(self):
        try:
            h = self.hook.get(TaskHookEnum.ON_FAILED)
            h()
        except Exception as ex:
            pass
        finally:
            self.status = TaskStatusEnum.FAILED

    def finally_hook(self):
        try:
            h = self.hook.get(TaskHookEnum.ON_FINALLY)
            h()
        except Exception as ex:
            pass
        finally:
            pass

    def __call__(self, func: Optional[Callable]):
        if func:
            self.func = func
        return self

    def run(self, *args, **kwargs):
        try:
            self.ready_hook()
            if args:
                self.func_args = args
            if kwargs:
                self.func_kw = kwargs
            rst = self.func(*self.func_args, **self.func_kw)

            if rst is False or rst == 1:
                raise Exception("task failed")
            self.success_hook()
        except Exception as ex:
            self.failed_hook()
        finally:
            self.finally_hook()

import datetime
import os.path

from spider.util.log import logger
import sys
import importlib
from spider.task import Task


def task_run_local(prj_path='./', task_names=None, task_sets=None):
    if task_names and task_sets:
        logger.warn("we will run task_names %s in task_sets %s" % (task_names, task_sets))

    if not task_names and not task_sets:
        raise Exception("task_names and task_sets both empty")

    # get tasks

    sys.path.insert(0, os.path.abspath(prj_path))
    task_m = importlib.import_module("task_entry")
    tasks = []
    task_set = 'default'
    for key, val in task_m.__dict__.items():
        if not isinstance(val, Task):
            continue

        _task: Task = val
        tasks.append(_task)

    if hasattr(task_m, '__TaskSet__'):
        task_set = getattr(task_m, '__TaskSet__')

    while True:
        for task in tasks:
            if task.cron.next_run < datetime.datetime.now()
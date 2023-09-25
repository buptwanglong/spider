import os.path
import sys
import importlib
import click
import requests

from spider.task import Task
from spider.util.common import local_cli_id, timestamp
from spider.protocol import Message, MessageTypeEnum, Task as TaskMsgData
from spider.client.config import load_conf


@click.command()
@click.argument("prj_path")
@click.option("--task_names")
def task_register(prj_path, task_names):
    prj_path = os.path.abspath(prj_path)
    sys.path.insert(0, prj_path)
    if task_names:
        task_names = str(task_names).split(",")
    else:
        task_names = []
    _conf = load_conf(os.path.join(prj_path, 'conf.yaml'))

    if not os.path.exists(prj_path):
        raise Exception("task search path not exist")

    if not os.path.isdir(prj_path):
        raise Exception("task search path should be dir")
    if not os.path.exists(os.path.join(prj_path, "task_entry.py")):
        raise Exception("project should has task_entry.py as task entry", os.path.join(prj_path, "task_entry.py"))

    if not os.path.exists(os.path.join(prj_path, "conf.yaml")):
        raise Exception("project should has conf.yaml to configure the task")

    task_m = importlib.import_module("task_entry")

    msgs = []
    for key, value in task_m.__dict__.items():
        if not isinstance(value, Task):
            continue

        _task: Task = value
        if task_names and _task.name not in task_names:
            continue
        m = Message(
            m_type=MessageTypeEnum.TASK,
            data=TaskMsgData(
                name=_task.name,
                repo=_conf.repo,
                task_set=getattr(task_m, '__TaskSet__', 'default'),
                task_conf=_conf.task_conf,
                cron=_task.cron.cron_str
            ),
            client_id=local_cli_id(),
            timestamp=timestamp()
        )
        msgs.append(m)

        resp = requests.post(f"http://{_conf.web2host}:{_conf.web2port}/tasks/register", json=msgs)
        resp.raise_for_status()
        if resp.json()['code'] != 0:
            click.echo("register err resp %s" % resp.json())


if __name__ == '__main__':
    prj_path = './'
    task_register(prj_path)

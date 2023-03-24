import os
import click
from bin.cli import cli

conf_yaml = """
repo:
  type: git
  info:
    url: git@xx.com
    branch: master
    commit: latest
    access_token: xxxx
task_conf:
  work_dir: ./
  env:
    python_version: 3.7
    requirements_location: requirements.txt

web:
  host: xxx.xxx.com
  port: xxx
"""

requiremetns_txt = """

"""

task_entry = """
from spider.task import Task

__TaskSet__ = "Default"


@Task(retry=1, name='hello', cron='1/* * * *')
def d():
    print('ab')


if __name__ == '__main__':
    pass

"""


@cli.command()
@click.argument("location")
def project_init(location):
    if not location:
        raise Exception("prj init location should not empty")

    abs_path = os.path.abspath(location)

    if not os.path.isdir(abs_path):
        raise Exception("prj init need a dir path")

    if not os.path.exists(abs_path):
        os.mkdir(abs_path)

    with open(os.path.join(abs_path, "conf.yaml"), "w") as f:
        f.write(conf_yaml)

    with open(os.path.join(abs_path, "requirements.txt"), "w") as f:
        f.write(requiremetns_txt)

    with open(os.path.join(abs_path, "task_entry.py"), "w") as f:
        f.write(task_entry)

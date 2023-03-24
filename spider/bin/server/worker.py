from spider.server.worker.worker import WorkerGroup
import signal
import os
import click
from bin.cli import cli

from spider.server.config import load_conf


@cli.group()
def worker():
    pass


@worker.command()
@click.option("--mod", default="soft")
@click.argument("pid")
def down(pid, mod):
    if mod == 'soft':
        os.kill(pid, signal.SIGTERM)
    else:
        os.kill(pid, signal.SIGKILL)
    pass


@worker.command()
@click.option("--conf")
def up(conf: str):
    load_conf(conf_path=conf)
    wg = WorkerGroup()
    wg.worker_server()



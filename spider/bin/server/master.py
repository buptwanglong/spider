from spider.server.master.master import Master
from spider.server.web.__main__ import web_run
import signal
import os
import click
import yaml
from bin.cli import cli

from spider.backend import SqlAlchemyBackend


@cli.group()
def master():
    pass


@master.command()
@click.option("--conf", required=True)
def up(conf: str):
    Master(conf_path=conf).entry()


@master.command()
@click.option("--mod", default="soft")
@click.argument("pid")
def down(pid, mod='soft'):
    if mod == 'soft':
        os.kill(pid, signal.SIGTERM)
    else:
        os.kill(pid, signal.SIGKILL)


@master.command()
@click.option("--conf")
def bk_init(conf: str):
    if os.path.isabs(conf):
        conf: dict = yaml.safe_load(open(conf, 'r'))
    else:
        conf: dict = yaml.safe_load(open(os.path.abspath(os.path.join(os.getcwd(), conf))))

    bk_url = conf.get("master", {}).get("backend", None)
    bk = SqlAlchemyBackend(bk_url=bk_url)
    bk.meta_init()


@cli.group()
@click.option("--conf")
def web(conf: str):
    if os.path.isabs(conf):
        conf: dict = yaml.safe_load(open(conf, 'r'))
    else:
        conf: dict = yaml.safe_load(open(os.path.abspath(os.path.join(os.getcwd(), conf))))

    web_host = conf.get("web", {}).get("host")
    web_port = conf.get("web", {}).get("port")

    if not web_host:
        raise Exception("web host should not be empty")

    if not web_port:
        raise Exception("web port should not be empty")

    web_run(web_host, web_port)

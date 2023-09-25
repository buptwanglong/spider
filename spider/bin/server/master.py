from spider.server.master.master import Master
from spider.server.web.__main__ import web_run
import signal
import os
import click
import yaml

from spider.backend import SqlAlchemyBackend, load_backend
from spider.server.config import load_conf


@click.group()
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


if __name__ == '__main__':
    master.main()
    pass

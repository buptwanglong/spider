import os.path

from spider.bin.cli import cli
import click
from spider.server.config import load_conf
from spider.server.web.__main__ import web_run


@cli.group()
def web():
    pass


@web.command()
@click.option("--conf")
def run(conf):
    web_run(conf)

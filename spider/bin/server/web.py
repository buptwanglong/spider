import click
from spider.server.web.__main__ import web_run


@click.group()
def web():
    pass


@web.command()
@click.option("--conf")
def run(conf):
    web_run(conf)

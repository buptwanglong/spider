import click

from spider.bin.server.master import master
from spider.bin.server.web import web
from spider.bin.server.worker import worker
from spider.bin.client.cli import client


@click.group()
def cli():
    pass


cli.add_command(master)
cli.add_command(web)
cli.add_command(worker)
cli.add_command(client)

#
# @cli.command()
# def a():
#     print('a')
#     pass


if __name__ == '__main__':
    cli.main()

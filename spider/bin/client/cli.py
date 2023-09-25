import click
from spider.bin.client.prj_init import project_init
from spider.bin.client.task_register import task_register


@click.group
def client():
    pass


client.add_command(project_init)
client.add_command(task_register)

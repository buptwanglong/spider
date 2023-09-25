from spider.task import Task

__TaskSet__ = "Default"


@Task(retry=1, name='hello', cron='1/* * * *')
def d():
    print('ab')


if __name__ == '__main__':
    pass

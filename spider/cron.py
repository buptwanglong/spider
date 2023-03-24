import datetime

from croniter import croniter


class Cron(object):
    def __init__(self, crontab: str):
        self._cron = crontab

    @property
    def cron_str(self):
        return self._cron

    @property
    def next_run(self) -> int:
        return int(croniter(self._cron, datetime.datetime.now()).get_next(datetime.datetime).timestamp())

    @property
    def last_run(self) -> int:
        return int(croniter(self._cron, datetime.datetime.now()).get_prev(datetime.datetime).timestamp())

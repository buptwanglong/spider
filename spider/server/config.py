import os.path
import yaml
from typing import Optional
from spider.util.memcache import call_cache
from spider.util.ctx import g_ctx


class ServerConf(object):
    def __init__(self, conf_path):
        self._conf = yaml.safe_load(open(os.path.abspath(conf_path)))
        self.master = self._conf.get('master', {})
        assert self.master, "empty master"
        self.master2host = self.master.get('host', '')
        assert self.master2host, "empty host"
        self.master2port = self.master.get('port', '')
        assert self.master2port, "empty port"
        self.master2backend = self.master.get('backend', '')
        assert self.master2backend, "empty bk"
        self.master2worker_dir = self.master.get('work_dir', '')
        assert self.master2worker_dir, 'empty worker dir'

        self.worker = self._conf.get('worker', {})
        assert self.worker, 'empty worker'
        self.woker2work_dir = self.worker.get('work_dir', '')
        assert self.woker2work_dir, 'empty work dir'
        self.worker2master_host = self.worker.get('master_host', '')
        self.worker2master_port = self.worker.get('master_port', '')
        assert self.worker2master_host, 'empty master host'
        assert self.worker2master_port, 'empty master port'

        self.web = self._conf.get('web', {})
        self.web2host = self.web.get('host', '')
        self.web2port = self.web.get('port', '')
        assert self.web2host, 'empty web host'
        assert self.web2port, 'empty web port'


@call_cache
def load_conf(conf_path) -> ServerConf:
    sc = ServerConf(conf_path=conf_path)
    g_ctx.add_or_update('server_conf', sc)
    return sc

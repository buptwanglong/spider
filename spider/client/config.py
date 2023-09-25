import yaml
import os
from typing import Optional
from spider.util.ctx import g_ctx
from spider.util.memcache import call_cache


class ClientConfig(object):
    def __init__(self, conf_path):
        self._conf = yaml.safe_load(open(os.path.abspath(conf_path)))
        self.repo = self._conf.get('repo', {})
        assert self.repo, "repo should not be empty"
        self.repo2type = self.repo.get('type')
        assert self.repo2type, "repo type should not be empty"
        self.repo2info = self.repo.get('info', {})
        assert self.repo2info, "repo info should not be empty"
        self.task_conf = self._conf.get('task_conf', {})
        assert self.task_conf, "task conf should not be empty"
        self.task_conf2work_dir = self.task_conf.get('work_dir')
        assert self.task_conf2work_dir, "task conf -> work dir should not be empty"

        self.task_conf2env = self.task_conf.get('env', {})
        assert self.task_conf2env, "task conf -> env should not be empty"

        self.web = self._conf.get('web', {})
        assert self.web, "web should not be empty"
        self.web2host = self.web.get('host')
        assert self.web2host, "web host should not be empty"
        self.web2port = self.web.get('port')
        assert self.web2port, "web port should not be empty"


@call_cache
def load_conf(conf_path) -> ClientConfig:
    cc = ClientConfig(conf_path=conf_path)
    g_ctx.add_or_update('client_conf', cc)
    return cc

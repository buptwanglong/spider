import datetime
import os.path

from spider.protocol import Task, TaskLog, TaskStatus, Message, MessageTypeEnum
from spider.util.memcache import cached_property
from urllib.parse import urlparse
from spider.util.github import GitHubCli
import subprocess
from spider.util.ctx import g_ctx
from spider.server.config import ServerConf
from spider.util.listener import inject
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from spider.server.worker.worker import WorkerGroup


class RepoValidate(object):
    def __init__(self, **kwargs):
        self.url = kwargs.pop('url') or None
        self.branch = kwargs.pop("branch") or 'master'
        if not self.url:
            raise Exception("github url should not be empty")
        self.commit = kwargs.pop("commit") or "latest"
        self.access_token = kwargs.pop("access_token")

    @cached_property
    def repo(self):
        return urlparse(self.url).path

    @cached_property
    def host_name(self):
        return urlparse(self.url).hostname


class TaskConfValidate(object):
    def __init__(self, **kwargs):
        self.task_cwd = kwargs.pop("work_dir") or "."
        self.env = kwargs.pop("env") or {}

    @cached_property
    def py_version(self):
        return self.env.get("python_version")

    @cached_property
    def requirements(self):
        return self.env.get("requirements_location")


class WorkerProcessor(object):
    def __init__(self, task: Task, wg: WorkerGroup):
        self.task = task
        self.repo_validate = RepoValidate(**task.repo.get("info", {}))
        self.task_conf_validate = TaskConfValidate(**task.repo.get("task_conf", {}))
        self.task_log = TaskLog(
            name=task.name,
            task_set=task.task_set,
        )
        # inject listener
        inject(self.task_log, 'state', self.task_log_listener)
        self.wg = wg

    def task_log_listener(self):
        self.wg.client.send(Message(
            m_type=MessageTypeEnum.TASK_LOG,
            data=self.task_log,
            client_id=self.wg.id,
            timestamp=int(datetime.datetime.now().timestamp())
        ))

    @cached_property
    def conf(self) -> ServerConf:
        _conf = g_ctx.get('server_conf')
        if not _conf:
            raise Exception("conf is empty")
        return _conf

    @cached_property
    def work_dir(self):
        return self.conf.woker2work_dir

    @cached_property
    def task_path(self):
        return os.path.join(self.work_dir, self.repo_validate.repo)

    def load_code(self):
        GitHubCli(access_token=self.repo_validate.access_token,
                  host_name=self.repo_validate.host_name).pull2path(self.repo_validate.repo,
                                                                    path=self.work_dir)

        return

    def gen_dockerfile(self):
        dockerfile = f"""
         FROM python:{self.task_conf_validate.py_version}
         VOLUME {self.task_path} /home/{self.repo_validate.repo}
         WORKDIR /home/{self.repo_validate.repo}
         RUN pip install virtualenv \
         && virtualenv venv ./ \
         && source ./venv/bin/activate \
         && pip install -r requirements.txt 
         ENTRYPOINT ["spider worker task_run"]
         """
        with open(os.path.join(self.task_path, "Dockerfile"), 'w') as df:
            df.write(dockerfile)

    def build_docker_image(self):
        image_name = f"{self.repo_validate.repo}:{int(datetime.datetime.now().timestamp())}"
        subprocess.check_call(f"cd {self.task_path} && docker build -t {image_name} .")
        return image_name

    def docker_run(self, img_name):
        subprocess.check_call(f"docker run -it -rm {img_name} --task_dir . --task_name {self.task.name}")

    def core(self):
        try:
            self.task_log.state = TaskStatus.RUNNING
            self.load_code()
            self.gen_dockerfile()
            image_name = self.build_docker_image()
            self.docker_run(image_name)
            self.task_log.state = TaskStatus.SUCCESS

        except Exception as e:
            self.task_log.state = TaskStatus.FAILED
            raise Exception("work process task error", e)

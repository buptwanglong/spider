from gevent import monkey

monkey.patch_all()  # noqa
from gevent import pywsgi
import logging
import os
from flask import Flask
from spider.server.config import load_conf
from spider.backend import load_backend
from spider.server.web.api import tasks_register

app = Flask(__name__)
loger = logging.Logger(__file__)


@app.route('/tasks/register')
def tasks_register():
    return tasks_register.tasks_register()


def web_run(conf):
    conf = load_conf(conf_path=os.path.abspath(conf))
    load_backend(conf.master2backend)

    server = pywsgi.WSGIServer((conf.web2host, conf.web2port), app)
    server.serve_forever()


if __name__ == '__main__':
    pass

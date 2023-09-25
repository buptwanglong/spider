from gevent import pywsgi
from flask import Flask
from spider.server.web.api import tasks as t
from spider.server.config import load_conf

app = Flask(__name__)


@app.route('/tasks/register', methods=['POST'])
def tasks_register():
    return t.tasks_register()


def web_run(conf: str):
    _conf = load_conf(conf)
    app.conf = conf
    server = pywsgi.WSGIServer((_conf.web2host, _conf.web2port), app)
    server.serve_forever()


if __name__ == '__main__':
    conf_path = "/Users/wanglong/projects/spider/examples/example_server_a/conf.yaml"
    web_run(conf_path)
    pass

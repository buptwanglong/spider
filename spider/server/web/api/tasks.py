from flask import request
import logging
from spider import backend
import json
from protocol import Message
from spider.util.ctx import g_ctx

loger = logging.Logger(__file__)


def tasks_register():
    global_sc = g_ctx['server_conf']
    try:
        msgs = [Message.unserialize(json.dumps(msg)) for msg in request.json]

    except Exception as ex:
        loger.error("Task data error", ex)
        return {
            "code": -1,
            "data": None
        }
    else:
        try:
            for msg in msgs:
                backend.load_backend(global_sc.master2backend).task_add(msg)
            return {
                "code": 0,
                "data": None
            }
        except Exception as ex:
            loger.error("save model err", ex)
            return {
                "code": -1,
                "data": None
            }


def task_on():
    global_sc = g_ctx['server_conf']
    try:
        req_data = request.json.get('data', {})
        names = req_data.get('task_names', [])
        task_set = req_data.get('task_set', None)
        if not names and not task_set:
            raise Exception("names and task_set both empty")

        backend.load_backend(global_sc.master2backend).task_add()

    except Exception as e:
        loger.error("task on err", e)
        return {
            "code": -1,
            "data": None
        }


def task_off():
    pass


def task_delete():
    pass

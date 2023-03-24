from flask import request
from spider.protocol import Task
import logging
from spider.backend import BACKEND

loger = logging.Logger(__file__)


def tasks_register():
    try:
        msgs = [msg for msg in request.json()]

    except Exception as ex:
        loger.error("Task data error", ex)
        return {
            "code": -1,
            "data": None
        }
    else:
        try:
            for msg in msgs:
                BACKEND.task_add(msg)
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

from typing import Optional, Callable


def inject(obj, attr, hook: Optional[Callable] = None):
    if not hasattr(obj, attr):
        raise Exception("obj %s has no attr %s" % (obj, attr))

    def __def_setattr__(self, key, value):
        print(self)
        print(type(self))

        if key != attr:
            return super(obj.__class__, self).__setattr__(key, value)
        else:
            hook()
            return super(obj.__class__, self).__setattr__(key, value)

    obj.__class__.__setattr__ = __def_setattr__


if __name__ == '__main__':
    import spider.protocol
    from spider.protocol import DictObj, Message, MessageTypeEnum, Worker
    from importlib import reload

    d = Message(m_type=MessageTypeEnum.TASK, data=Worker(id=1, state='state'), client_id=1, timestamp=1)
    inject(d, 'm_type', hook=lambda: print('a'))
    d.m_type = MessageTypeEnum.WORKER
    d.client_id = 2

    # from spider.protocol import DictObj, Message, MessageTypeEnum, Worker

    # reload(spider.protocol)
    # dd = Message(m_type=MessageTypeEnum.TASK, data=Worker(id=1, state='state'), client_id=1, timestamp=1)
    # dd.m_type = MessageTypeEnum.WORKER
    # dd.client_id = 2
    #
    # print(d)

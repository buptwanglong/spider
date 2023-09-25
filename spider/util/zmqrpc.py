import zmq.green as zmq
from spider.util.retry import retry
from spider.protocol import Message


class BaseSocket(object):
    def __init__(self, socket_type):
        self.ctx = zmq.Context()
        self.socket = self.ctx.socket(socket_type)

        self.socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self.socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 30)

    @retry()
    def send(self, msg: Message):
        resp = self.socket.send(msg.serialize().encode('utf8'))
        print(resp)

    @retry()
    def send2client(self, msg: Message):
        self.socket.send_multipart([msg.worker_id, msg.serialize()], zmq.NOBLOCK)

    def recv_from_client(self):
        data = self.socket.recv_multipart()
        node_id = data[0]
        msg = Message.unserialize(data[1])
        return bytes(node_id).decode('utf8'), msg

    @retry()
    def recv(self) -> Message:
        data = self.socket.recv()
        msg = Message.unserialize(data)
        return msg


class Server(BaseSocket):
    def __init__(self, host, port):
        super(Server, self).__init__(zmq.ROUTER)
        if port == 0:
            self.socket.bind_to_random_port(f"tcp://{host}")
        else:
            print("server", host, port)
            self.socket.bind(f"tcp://{host}:{port}")
            self.port = port


class Client(BaseSocket):
    def __init__(self, host, port: str, identity: str):
        # dealer need identity to identify the client
        super(Client, self).__init__(zmq.DEALER)
        self.socket.setsockopt(zmq.IDENTITY, identity.encode('utf8'))
        self.socket.connect("tcp://%s:%s" % (host, port))


if __name__ == '__main__':
    # c = Server(host='127.0.0.1', port='7777')
    # print(c.recv_from_client())

    c = Client(host='127.0.0.1', port='7777', identity='测试')
    from protocol import MessageTypeEnum

    c.send(Message(m_type=MessageTypeEnum.WORKER, data={'1': '测试'}, client_id='a', timestamp=1))

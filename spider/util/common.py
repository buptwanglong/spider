import datetime
import socket
import os


def extract_ip():
    st = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        st.connect(('10.255.255.255', 1))
        IP = st.getsockname()[0]
    except Exception:
        IP = '127.0.0.1'
    finally:
        st.close()
    return IP


def local_cli_id():
    return f"{extract_ip()}::{os.getpid()}"


def timestamp():
    return int(datetime.datetime.now().timestamp())




if __name__ == '__main__':
    print(local_cli_id())

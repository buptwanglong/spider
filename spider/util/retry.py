import gevent


def retry(delays=(1, 3, 5), exception=Exception):
    def decorator(function):
        def wrapper(*args, **kwargs):
            for delay in delays + (None,):
                try:
                    return function(*args, **kwargs)
                except exception as e:
                    if delay is None:
                        raise
                    else:
                        gevent.sleep(delay)

        return wrapper

    return decorator

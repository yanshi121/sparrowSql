import requests


class Sparrow(object):
    def __init__(self, host, port):
        self._url_ = f"http://{host}:{port}"

    def get(self, key):
        sql = f"get {key}"
        r = requests.post(self._url_, data={"command": sql})
        r.close()
        return r.json()

    def set(self, key, value, valid_time=None):
        if valid_time is None:
            sql = f"set {key} {value}"
        else:
            sql = f"set {key} {value} {valid_time}"
        r = requests.post(self._url_, data={"command": sql})
        r.close()
        return r.json()

    def get_all(self):
        sql = "get_all"
        r = requests.post(self._url_, data={"command": sql})
        r.close()
        return r.json()


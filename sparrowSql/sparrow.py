import requests


class Sparrow(object):
    def __init__(self, host: str, port: int, password: str, username: str):
        """
        Sparrow数据库连接类
        :param host: 地址
        :param port: 端口
        :param password: 密码
        :param username: 用户名
        """
        self.headers = {"SparrowApi": "SparrowApi", "Password": password, "Username": username}
        self._url_ = f"http://{host}:{port}"

    def get_key(self, key: str):
        """
        获取普通键值
        :param key: 键名
        :return:
        """
        sql = f"get {key}"
        r = requests.post(self._url_, data={"command": sql}, headers=self.headers)
        r.close()
        return r.json()

    def set_key(self, key: str, value: str, valid_time: float = None):
        """
        插入普通键值
        :param valid_time: 过期时间，不输入则不过期
        :param value: 键值
        :param key: 键名
        :return:
        """
        if valid_time is None:
            sql = f"set {key} {value}"
        else:
            sql = f"set {key} {value} {valid_time}"
        r = requests.post(self._url_, data={"command": sql}, headers=self.headers)
        r.close()
        return r.json()

    def reset_key(self, key: str, value: str, valid_time: float = None):
        """
        修改普通键值
        :param valid_time: 过期时间，不输入则不过期
        :param value: 键值
        :param key: 键名
        :return:
        """
        if valid_time is None:
            sql = f"reset {key} {value}"
        else:
            sql = f"reset {key} {value} {valid_time}"
        r = requests.post(self._url_, data={"command": sql}, headers=self.headers)
        r.close()
        return r.json()

    def delete_key(self, key: str):
        """
        删除普通键值
        :param key: 键名
        :return:
        """
        sql = f"delete {key}"
        r = requests.post(self._url_, data={"command": sql}, headers=self.headers)
        r.close()
        return r.json()

    def set_body(self, key: str, body: dict, valid_time: int = None):
        """
        插入字典类型
        :param valid_time: 过期时间，不输入则不过期
        :param body: 值
        :param key: 键名
        :return:
        """
        body_str = ""
        for k, v in body.items():
            body_str += f"{k}={v},"
        if valid_time is None:
            sql = f"set_body {key} {body_str[0:-1]}"
        else:
            sql = f"set_body {key} {body_str[0:-1]} {valid_time}"
        r = requests.post(self._url_, data={"command": sql}, headers=self.headers)
        r.close()
        return r.json()

    def get_body(self, key: str):
        """
        获取字典类型
        :param key: 键名
        :return:
        """
        sql = f"get_body {key}"
        r = requests.post(self._url_, data={"command": sql}, headers=self.headers)
        r.close()
        return r.json()

    def reset_body(self, body_name: str, key: str, value: str, valid_time: int = None):
        """
        修改字典类型的其中一个值
        :param value: 被修改的数据值
        :param body_name: 键名
        :param valid_time: 过期时间，不输入则不过期
        :param key: 被修改的数据键
        :return:
        """
        if valid_time is None:
            sql = f"reset_body {body_name} {key}={value}"
        else:
            sql = f"reset_body {body_name} {key}={value} {valid_time}"
        print(sql)
        r = requests.post(self._url_, data={"command": sql}, headers=self.headers)
        r.close()
        return r.json()

    def reset_body_all(self, key: str, body: dict, valid_time: int = None):
        """
        修改字典类型的所有值
        :param valid_time: 过期时间，不输入则不过期
        :param body: 值
        :param key: 键名
        :return:
        """
        body_str = ""
        for k, v in body.items():
            body_str += f"{k}={v},"
        if valid_time is None:
            sql = f"reset_body_all {key} {body_str[0:-1]}"
        else:
            sql = f"reset_body_all {key} {body_str[0:-1]} {valid_time}"
        r = requests.post(self._url_, data={"command": sql}, headers=self.headers)
        r.close()
        return r.json()

    def delete_body(self, key: str):
        """
        删除字典类型
        :param key: 键名
        :return:
        """
        sql = f"delete_body {key}"
        r = requests.post(self._url_, data={"command": sql}, headers=self.headers)
        r.close()
        return r.json()

    def set_time(self, key: str, valid_time: float):
        """
        修改普通类型的过期时间
        :param key: 键名
        :param valid_time: 过期时间
        :return:
        """
        sql = f"set_time {key} {valid_time}"
        r = requests.post(self._url_, data={"command": sql}, headers=self.headers)
        r.close()
        return r.json()

    def set_body_time(self, key: str, valid_time: float):
        """
        修改字典类型的过期时间
        :param key: 键名
        :param valid_time: 过期时间
        :return:
        """
        sql = f"set_body_time {key} {valid_time}"
        r = requests.post(self._url_, data={"command": sql}, headers=self.headers)
        r.close()
        return r.json()

    def get_all(self):
        """
        获取全部普通类型数据
        :return:
        """
        sql = "get_all"
        r = requests.post(self._url_, data={"command": sql}, headers=self.headers)
        r.close()
        return r.json()

    def get_all_body(self):
        """
        获取全部字典类型的数据
        :return:
        """
        sql = "get_all_body"
        r = requests.post(self._url_, data={"command": sql}, headers=self.headers)
        r.close()
        return r.json()



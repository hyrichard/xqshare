"""远程调用的参数与返回值传输：list / dict 参数一次性取回服务端，numpy 返回值转为 Python 类型。

用本机回环的真实 rpyc 连接验证（客户端传 list / dict 时服务端收到的是远程引用）。
"""

import json
import threading
import time

import numpy as np
import pytest
import rpyc
from rpyc.core.netref import BaseNetref
from rpyc.utils.server import ThreadedServer

from xqshare.client import _deserialize_from_transfer
from xqshare.server import LoggingProxy, _init_logging, _json_default, _localize_call, _serialize_for_transfer

_init_logging("WARNING")
CONFIG = {"allow_public_attrs": True, "allow_pickle": True}


class _Target:
    """被代理的 xtdata 替身：记录收到的参数类型。"""

    def __init__(self):
        self.received = None

    def get_full_tick(self, code_list, field_list=None):
        self.received = (code_list, field_list)
        return {code: {"lastPrice": np.float64(1.5), "askPrice": np.array([1.6, 1.7])} for code in code_list}


class _Service(rpyc.Service):
    target = _Target()

    def exposed_proxy(self):
        return LoggingProxy(_Service.target, "xtdata", lambda: "test")

    def exposed_localize(self, *args, **kwargs):
        args, kwargs = _localize_call(args, kwargs)
        values = list(args) + list(kwargs.values())
        return tuple((type(value).__name__, isinstance(value, BaseNetref)) for value in values)


@pytest.fixture(scope="module")
def conn():
    server = ThreadedServer(_Service, port=0, protocol_config=CONFIG)
    thread = threading.Thread(target=server.start, daemon=True)
    thread.start()
    time.sleep(0.3)
    connection = rpyc.connect("127.0.0.1", server.port, config=CONFIG)
    yield connection
    connection.close()
    server.close()


def test_list_and_dict_arguments_become_local(conn):
    result = conn.root.localize(["000001.SZ", "600000.SH"], {"a": [1, 2]}, field_list=["open"])
    assert [kind for kind, remote in result] == ["list", "dict", "list"]
    assert not any(remote for _, remote in result)


def test_non_literal_arguments_stay_remote(conn):
    """含函数等非字面量元素时无法还原，保持远程引用（与原来行为相同）。"""
    result = conn.root.localize([print], {"callback": print})
    assert all(remote for _, remote in result)


def test_proxy_receives_local_list_quickly(conn):
    codes = [f"{i:06d}.SH" for i in range(8000)]
    begin = time.perf_counter()
    raw = conn.root.proxy().get_full_tick(codes, field_list=["lastPrice"])
    elapsed = time.perf_counter() - begin
    code_list, field_list = _Service.target.received
    assert type(code_list) is list and type(field_list) is list and len(code_list) == 8000
    assert elapsed < 0.5  # 逐项远程访问时本机回环也要约 1 秒
    ticks = json.loads(raw["data"])
    assert ticks["000001.SH"] == {"lastPrice": 1.5, "askPrice": [1.6, 1.7]}  # numpy 转为数字和列表，不是字符串


def test_json_default_converts_numpy_and_falls_back_to_str():
    assert _json_default(np.int64(3)) == 3 and _json_default(np.array([1, 2])) == [1, 2]
    assert _json_default(object).startswith("<class")
    assert _deserialize_from_transfer(_serialize_for_transfer([np.float64(2.5)])) == [2.5]

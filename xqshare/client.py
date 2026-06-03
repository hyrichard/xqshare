"""
XtQuant Share (xqshare) Client - Transparent remote proxy for xtquant
"""

import json
import logging
import os
import ssl
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from itertools import islice
from typing import Any, Callable, Dict, List, Optional, Tuple

import rpyc
from rpyc.utils.helpers import BgServingThread

# 默认客户端配置（与服务端保持一致）
DEFAULT_CLIENT_ID = "client-standard"
DEFAULT_CLIENT_SECRET = "xqshare-default-secret"
SERIALIZED_MARKER = "__xqshare_serialized__"
FORMULA_SUBSCRIBE_METHODS = {"subscribe_formula"}
FORMULA_REQUEST_METHODS = {"get_formula_result", "bind_formula", "unsubscribe_formula"}


# ==================== 日志配置 ====================

def setup_logging(log_level: str = "INFO", quiet: bool = False):
    """配置客户端日志"""
    log_dir = os.environ.get("XQSHARE_LOG_DIR", "logs")
    os.makedirs(log_dir, exist_ok=True)

    formatter = logging.Formatter(
        fmt='%(asctime)s.%(msecs)03d | %(levelname)-8s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    root_logger = logging.getLogger('xtquant_client')
    root_logger.setLevel(getattr(logging, log_level.upper()))

    if not quiet and not any(isinstance(h, logging.StreamHandler) for h in root_logger.handlers):
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.INFO)
        root_logger.addHandler(console_handler)

    log_path = os.path.join(log_dir, f"client_{datetime.now().strftime('%Y%m%d')}.log")
    if not any(isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", "") == os.path.abspath(log_path)
               for h in root_logger.handlers):
        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.DEBUG)
        root_logger.addHandler(file_handler)

    return root_logger


_logger = None
_quiet_mode = False


def set_quiet_mode(quiet: bool = True):
    """设置静默模式"""
    global _quiet_mode
    _quiet_mode = quiet


def get_logger():
    global _logger
    if _logger is None:
        _logger = setup_logging(quiet=_quiet_mode)
    return _logger


def _is_callback_debug_enabled() -> bool:
    """判断是否开启 callback 调试日志。"""
    value = os.environ.get("XQSHARE_DEBUG_CALLBACK", "")
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _summarize_callback_value(value: Any, max_len: int = 120) -> str:
    """生成回调参数摘要，避免把大对象完整写入日志。"""
    try:
        if value is None:
            return "None"
        if isinstance(value, (int, float, bool)):
            return str(value)
        if isinstance(value, str):
            return value[:max_len] if len(value) <= max_len else value[:max_len] + "..."
        if isinstance(value, dict):
            keys = list(islice(value.keys(), 5))
            return f"dict[{len(value)} keys:{', '.join(map(str, keys))}{'...' if len(value) > 5 else ''}]"
        if isinstance(value, (list, tuple, set)):
            return f"{type(value).__name__}[len={len(value)}]"
        if hasattr(value, "__class__"):
            return f"<{value.__class__.__name__}>"
        return str(type(value))
    except Exception:
        return "<unserializable>"


def _summarize_callback_payload(args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> str:
    """生成回调入参摘要。"""
    parts = []
    if args:
        parts.append(f"args={_summarize_callback_value(args if len(args) != 1 else args[0])}")
    if kwargs:
        parts.append(f"kwargs={_summarize_callback_value(kwargs)}")
    return " ".join(parts) if parts else "no_payload"


def _log_callback_debug(logger_obj: logging.Logger, phase: str, **fields: Any) -> None:
    """输出统一格式的 callback 调试日志。"""
    if not _is_callback_debug_enabled():
        return

    payload = {
        "thread": threading.current_thread().name,
        **fields,
    }
    message = " ".join(f"{key}={value}" for key, value in payload.items() if value is not None)
    logger_obj.info(f"[CB][CLIENT][{phase}] {message}")


# ==================== 反序列化传输数据 ====================

def _deserialize_from_transfer(result):
    """反序列化服务端优化传输的数据"""
    if not isinstance(result, dict) or SERIALIZED_MARKER not in result:
        return result

    serialized_type = result[SERIALIZED_MARKER]
    data = result["data"]

    if serialized_type == "none":
        return None

    if serialized_type == "json":
        return json.loads(data)

    if serialized_type == "dataframe_csv":
        import io
        try:
            import pandas as pd
            return pd.read_csv(io.StringIO(data), index_col=0)
        except ImportError:
            return data

    if serialized_type == "dict_with_dataframe":
        import io
        try:
            import pandas as pd

            def deserialize_dataframes(obj):
                if isinstance(obj, dict):
                    if obj.get("__df__"):
                        return pd.read_csv(io.StringIO(obj["csv"]), index_col=0)
                    return {k: deserialize_dataframes(v) for k, v in obj.items()}
                if isinstance(obj, list):
                    return [deserialize_dataframes(item) for item in obj]
                return obj

            deserialized = json.loads(data)
            return deserialize_dataframes(deserialized)
        except ImportError:
            return json.loads(data)

    return result


# ==================== 异常定义 ====================

class ConnectionError(Exception):
    """连接错误"""


class AuthenticationError(Exception):
    """认证错误"""


class CallbackError(Exception):
    """回调错误"""


# ==================== 重连策略 ====================

class ReconnectPolicy:
    """重连策略"""

    def __init__(self, max_retries=5, base_delay=1, max_delay=30, backoff_factor=2):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor

    def get_delay(self, retry_count):
        delay = self.base_delay * (self.backoff_factor ** retry_count)
        return min(delay, self.max_delay)


# ==================== 回调状态 ====================

@dataclass
class CallbackBinding:
    callback_id: str
    callback: Any
    kind: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    one_shot: bool = False
    active: bool = True


class CallbackRegistry:
    """客户端本地回调注册中心。"""

    def __init__(self, logger=None):
        self._callbacks: Dict[str, CallbackBinding] = {}
        self._counter = 0
        self._lock = threading.RLock()
        self._logger = logger or get_logger()

    def _new_id(self, prefix: str) -> str:
        with self._lock:
            self._counter += 1
            return f"{prefix}_{self._counter}"

    def register(
        self,
        callback: Any,
        kind: str,
        metadata: Optional[Dict[str, Any]] = None,
        one_shot: bool = False,
        callback_id: Optional[str] = None,
        prefix: str = "cb",
    ) -> str:
        binding_id = callback_id or self._new_id(prefix)
        with self._lock:
            self._callbacks[binding_id] = CallbackBinding(
                callback_id=binding_id,
                callback=callback,
                kind=kind,
                metadata=metadata or {},
                one_shot=one_shot,
            )
        return binding_id

    def unregister(self, callback_id: str):
        with self._lock:
            self._callbacks.pop(callback_id, None)

    def invoke(self, callback_id: str, *args, **kwargs):
        with self._lock:
            binding = self._callbacks.get(callback_id)
        if binding is None or not binding.active:
            raise CallbackError(f"回调不存在或已失效: {callback_id}")

        debug_enabled = _is_callback_debug_enabled()
        callback_name = None
        start_time = None
        if debug_enabled:
            callback_name = getattr(binding.callback, "__name__", binding.callback.__class__.__name__)
            start_time = time.perf_counter()
            _log_callback_debug(
                self._logger,
                "USER_CB_START",
                callback_id=callback_id,
                kind=binding.kind,
                handler=callback_name,
                payload=_summarize_callback_payload(args, kwargs),
            )
        try:
            result = binding.callback(*args, **kwargs)
            if debug_enabled:
                elapsed_ms = (time.perf_counter() - start_time) * 1000
                _log_callback_debug(
                    self._logger,
                    "USER_CB_DONE",
                    callback_id=callback_id,
                    kind=binding.kind,
                    handler=callback_name,
                    cost_ms=f"{elapsed_ms:.2f}",
                    result=_summarize_callback_value(result),
                )
            return result
        except Exception as exc:
            if debug_enabled:
                elapsed_ms = (time.perf_counter() - start_time) * 1000
                _log_callback_debug(
                    self._logger,
                    "USER_CB_ERROR",
                    callback_id=callback_id,
                    kind=binding.kind,
                    handler=callback_name,
                    cost_ms=f"{elapsed_ms:.2f}",
                    error=type(exc).__name__,
                    message=str(exc)[:200],
                )
            raise
        finally:
            if binding.one_shot:
                self.unregister(callback_id)

    def invoke_event(self, callback_id: str, event_name: str, *args, **kwargs):
        with self._lock:
            binding = self._callbacks.get(callback_id)
        if binding is None or not binding.active:
            raise CallbackError(f"回调不存在或已失效: {callback_id}")

        handler = getattr(binding.callback, event_name, None)
        if not callable(handler):
            self._logger.debug(f"忽略未实现的交易回调: {event_name} | binding={callback_id}")
            return None

        debug_enabled = _is_callback_debug_enabled()
        handler_name = None
        start_time = None
        if debug_enabled:
            handler_name = f"{binding.callback.__class__.__name__}.{event_name}"
            start_time = time.perf_counter()
            _log_callback_debug(
                self._logger,
                "USER_CB_START",
                callback_id=callback_id,
                kind=binding.kind,
                event=event_name,
                handler=handler_name,
                payload=_summarize_callback_payload(args, kwargs),
            )
        try:
            result = handler(*args, **kwargs)
            if debug_enabled:
                elapsed_ms = (time.perf_counter() - start_time) * 1000
                _log_callback_debug(
                    self._logger,
                    "USER_CB_DONE",
                    callback_id=callback_id,
                    kind=binding.kind,
                    event=event_name,
                    handler=handler_name,
                    cost_ms=f"{elapsed_ms:.2f}",
                    result=_summarize_callback_value(result),
                )
            return result
        except Exception as exc:
            if debug_enabled:
                elapsed_ms = (time.perf_counter() - start_time) * 1000
                _log_callback_debug(
                    self._logger,
                    "USER_CB_ERROR",
                    callback_id=callback_id,
                    kind=binding.kind,
                    event=event_name,
                    handler=handler_name,
                    cost_ms=f"{elapsed_ms:.2f}",
                    error=type(exc).__name__,
                    message=str(exc)[:200],
                )
            raise

    def list_active(self):
        with self._lock:
            return {k: v for k, v in self._callbacks.items() if v.active}

    def clear(self):
        with self._lock:
            self._callbacks.clear()


@dataclass
class SubscriptionState:
    client_seq: int
    server_seq: Any
    method_name: str
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]
    callback_id: str
    active: bool = True


@dataclass
class TraderModuleState:
    userdata_path: Optional[str]
    session_id: Optional[int]
    module: Any
    callback_binding_id: Optional[str] = None
    start_args: Tuple[Any, ...] = field(default_factory=tuple)
    start_kwargs: Dict[str, Any] = field(default_factory=dict)
    connect_args: Tuple[Any, ...] = field(default_factory=tuple)
    connect_kwargs: Dict[str, Any] = field(default_factory=dict)
    started: bool = False
    connected: bool = False
    subscriptions: list = field(default_factory=list)


# ==================== 远程模块代理 ====================

class RemoteModule:
    """远程模块代理 - 完全透明的动态代理"""

    def __init__(self, client, module_name, module=None):
        self._client = client
        self._module_name = module_name
        self._module = module
        self._logger = get_logger()
        self._trader_state: Optional[TraderModuleState] = None

    def _ensure_module(self):
        if self._module is None:
            self._client._ensure_connected()
            method = getattr(self._client._conn.root, f'get_{self._module_name}')
            self._module = method()
        return self._module

    def __getattr__(self, name):
        module = self._ensure_module()
        try:
            attr = getattr(module, name)
            if callable(attr):
                return self._wrap_call(attr, name)
            return attr
        except Exception as e:
            if self._client._should_reconnect(e):
                self._module = None
                module = self._ensure_module()
                attr = getattr(module, name)
                if callable(attr):
                    return self._wrap_call(attr, name)
                return attr
            raise

    def _wrap_call(self, func, func_name: str):
        def wrapper(*args, **kwargs):
            if self._module_name == "xtdata":
                if func_name.startswith("subscribe"):
                    callback, _, _ = self._client._extract_callback(args, kwargs)
                    if callback is not None:
                        return self._client._call_xtdata_subscribe(func_name, args, kwargs)
                if func_name.startswith("unsubscribe"):
                    return self._client._call_xtdata_unsubscribe(func_name, func, args, kwargs)
                if func_name in FORMULA_REQUEST_METHODS:
                    args, kwargs = self._client._translate_formula_request(args, kwargs)

            if self._module_name == "xttrader":
                if func_name == "register_callback":
                    return self._client._call_trader_register_callback(self, args, kwargs)
                if func_name.endswith("_async"):
                    callback, _, _ = self._client._extract_callback(args, kwargs)
                    if callback is not None:
                        return self._client._call_trader_async(self, func_name, args, kwargs)

            start_time = time.perf_counter()
            args_str = self._summarize_args(args, kwargs)
            self._logger.info(f"[CALL] {self._module_name}.{func_name}({args_str})")

            try:
                result = func(*args, **kwargs)
                result = _deserialize_from_transfer(result)
                elapsed_ms = (time.perf_counter() - start_time) * 1000
                result_summary = self._summarize_result(result)
                self._logger.info(f"[OK] {self._module_name}.{func_name} | {elapsed_ms:.2f}ms | {result_summary}")
                self._record_trader_state(func_name, args, kwargs)
                return result
            except Exception as e:
                elapsed_ms = (time.perf_counter() - start_time) * 1000
                self._logger.error(f"[ERROR] {self._module_name}.{func_name} | {elapsed_ms:.2f}ms | {type(e).__name__}: {e}")
                raise

        wrapper.__name__ = func_name
        wrapper.__qualname__ = f"{self._module_name}.{func_name}"
        return wrapper

    def _summarize_args(self, args, kwargs, max_len: int = 100) -> str:
        parts = []
        if args:
            for arg in args[:3]:
                try:
                    parts.append(str(arg)[:30])
                except Exception:
                    parts.append("?")
            if len(args) > 3:
                parts.append(f"...+{len(args) - 3}")
        if kwargs:
            for k, v in list(kwargs.items())[:2]:
                try:
                    parts.append(f"{k}={str(v)[:20]}")
                except Exception:
                    parts.append(f"{k}=?")
            if len(kwargs) > 2:
                parts.append(f"...+{len(kwargs) - 2}")
        return ", ".join(parts)[:max_len]

    def _summarize_result(self, result, max_len: int = 100) -> str:
        try:
            if result is None:
                return "None"
            if isinstance(result, (int, float, bool)):
                return str(result)
            if isinstance(result, str):
                return result[:max_len] if len(result) > max_len else result
            if isinstance(result, (list, tuple)):
                return f"{type(result).__name__}[len={len(result)}]"
            if isinstance(result, dict):
                return f"dict[{len(result)} keys]"
            return f"<{type(result).__name__}>"
        except Exception:
            return "?"

    def __dir__(self):
        module = self._ensure_module()
        return dir(module)

    def _record_trader_state(self, func_name: str, args, kwargs):
        state = self._trader_state
        if self._module_name != "xttrader" or state is None:
            return

        if func_name == "start":
            state.started = True
            state.start_args = tuple(args)
            state.start_kwargs = dict(kwargs)
        elif func_name == "connect":
            state.connected = True
            state.connect_args = tuple(args)
            state.connect_kwargs = dict(kwargs)
        elif func_name == "subscribe":
            state.subscriptions.append((tuple(args), dict(kwargs)))
        elif func_name == "unsubscribe":
            to_remove = (tuple(args), dict(kwargs))
            state.subscriptions = [item for item in state.subscriptions if item != to_remove]
        elif func_name == "stop":
            state.started = False
            state.connected = False
            state.subscriptions = []


# ==================== 主客户端类 ====================

class XtQuantRemote:
    """远程 xtquant 完全透明代理。"""

    def __init__(
        self,
        host=None,
        port=None,
        client_id=None,
        client_secret=None,
        use_ssl=False,
        ssl_verify=True,
        auto_reconnect=True,
        max_retries=5,
        heartbeat_interval=30,
        log_level="INFO",
        env_file=None,
    ):
        try:
            from dotenv import load_dotenv
            load_dotenv(env_file)
        except ImportError:
            pass

        if host is None:
            host = os.environ.get("XQSHARE_REMOTE_HOST", "localhost")
        if port is None:
            port = int(os.environ.get("XQSHARE_REMOTE_PORT", "18812"))
        if client_id is None:
            client_id = os.environ.get("XQSHARE_CLIENT_ID", DEFAULT_CLIENT_ID)
        if client_secret is None:
            client_secret = os.environ.get("XQSHARE_CLIENT_SECRET", DEFAULT_CLIENT_SECRET)

        self._host = host
        self._port = port
        self._client_id = client_id
        self._client_secret = client_secret
        self._use_ssl = use_ssl
        self._ssl_verify = ssl_verify
        self._auto_reconnect = auto_reconnect
        self._reconnect_policy = ReconnectPolicy(max_retries=max_retries)
        self._heartbeat_interval = heartbeat_interval
        self._log_level = log_level

        self._conn = None
        self._authenticated = False
        self._connected = False
        self._reconnecting = False
        self._heartbeat_thread = None
        self._stop_heartbeat = threading.Event()
        self._bg_thread = None
        self._account_level = None

        self._logger = get_logger()
        self._callback_registry = CallbackRegistry(self._logger)
        self._subscriptions: Dict[Any, SubscriptionState] = {}
        self._subscription_lock = threading.RLock()
        self._next_client_seq = 1
        self._trader_states: List[TraderModuleState] = []

        self._xtdata = RemoteModule(self, 'xtdata')
        self._xttrader = RemoteModule(self, 'xttrader')
        self._xttype = RemoteModule(self, 'xttype')
        self._xtconstant = RemoteModule(self, 'xtconstant')
        self._xtview = RemoteModule(self, 'xtview')

        self._connect()

    def _should_reconnect(self, error):
        if not self._auto_reconnect:
            return False
        error_str = str(error).lower()
        hints = ['connection', 'closed', 'reset', 'broken', 'timeout', 'refused', 'eof', 'socket']
        return any(h in error_str for h in hints)

    def _create_ssl_context(self):
        if not self._use_ssl:
            return None
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        if self._ssl_verify:
            ctx.verify_mode = ssl.CERT_REQUIRED
            ctx.check_hostname = True
        else:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        return ctx

    def _connect(self):
        config = {
            'allow_public_attrs': True,
            'allow_pickle': True,
            'allow_getattr': True,
            'allow_setattr': True,
            'allow_delattr': True,
            'allow_all_attrs': True,
            'sync_request_timeout': 300,
        }

        ssl_context = self._create_ssl_context()

        try:
            try:
                self._conn = rpyc.connect(self._host, self._port, config=config, ssl_context=ssl_context)
            except TypeError:
                if ssl_context and self._use_ssl:
                    import socket
                    sock = socket.create_connection((self._host, self._port))
                    sock = ssl_context.wrap_socket(sock, server_hostname=self._host)
                    self._conn = rpyc.connect_stream(sock, config=config)
                else:
                    self._conn = rpyc.connect(self._host, self._port, config=config)

            self._connected = True
            self._bg_thread = BgServingThread(self._conn)
            self._logger.debug("后台服务线程已启动")

            if self._client_secret:
                result = self._conn.root.authenticate(self._client_id, self._client_secret)
                self._authenticated = True
                if isinstance(result, dict):
                    self._account_level = result.get("level", "free")
                    self._logger.info(f"认证成功: client_id={self._client_id} | level={self._account_level}")
                else:
                    self._logger.info(f"认证成功: client_id={self._client_id}")

            if self._heartbeat_interval > 0:
                self._start_heartbeat()

            self._logger.info(f"连接成功: {self._host}:{self._port}")
        except Exception as e:
            self._connected = False
            raise ConnectionError(f"连接失败: {e}")

    def _ensure_connected(self):
        if self._connected and self._conn:
            return
        if not self._auto_reconnect:
            raise ConnectionError("连接已断开，自动重连已禁用")
        self._reconnect()

    def _reconnect(self):
        if self._reconnecting:
            for _ in range(10):
                time.sleep(0.5)
                if self._connected:
                    return
            raise ConnectionError("重连超时")

        self._reconnecting = True
        retry_count = 0

        try:
            while retry_count < self._reconnect_policy.max_retries:
                try:
                    self._logger.info(f"重连中... 第 {retry_count + 1} 次尝试")

                    if self._conn:
                        try:
                            self._conn.close()
                        except Exception:
                            pass

                    self._conn = None
                    self._connected = False
                    self._reset_remote_modules()
                    self._connect()
                    self._restore_traders_after_reconnect()
                    self._restore_subscriptions_after_reconnect()
                    self._logger.info("重连成功")
                    return
                except Exception as e:
                    retry_count += 1
                    delay = self._reconnect_policy.get_delay(retry_count - 1)
                    self._logger.warning(f"重连失败: {e}，{delay}秒后重试...")
                    time.sleep(delay)

            raise ConnectionError(f"重连失败，已尝试 {retry_count} 次")
        finally:
            self._reconnecting = False

    def _start_heartbeat(self):
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            return
        self._stop_heartbeat.clear()
        self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._heartbeat_thread.start()

    def _heartbeat_loop(self):
        while not self._stop_heartbeat.is_set():
            try:
                if self._connected and self._conn:
                    try:
                        self._conn.root.heartbeat()
                    except Exception as e:
                        if self._auto_reconnect:
                            self._logger.warning(f"心跳失败: {e}，尝试重连...")
                            try:
                                self._reconnect()
                            except Exception:
                                pass
            except Exception:
                pass
            self._stop_heartbeat.wait(self._heartbeat_interval)

    def _stop_heartbeat_thread(self):
        self._stop_heartbeat.set()
        if self._heartbeat_thread:
            self._heartbeat_thread.join(timeout=2)

    def _reset_remote_modules(self):
        """重连前清空远端模块缓存，避免继续持有旧连接上的 netref。"""
        for module in (self._xtdata, self._xttrader, self._xttype, self._xtconstant, self._xtview):
            module._module = None

    def _next_seq(self) -> int:
        with self._subscription_lock:
            seq = self._next_client_seq
            while seq in self._subscriptions:
                seq += 1
            self._next_client_seq = seq + 1
            return seq

    def _extract_callback(self, args, kwargs, object_mode: bool = False):
        args = tuple(args)
        kwargs = dict(kwargs)

        if "callback" in kwargs:
            callback = kwargs.pop("callback")
            return callback, args, kwargs

        if object_mode:
            if args:
                return args[0], args[1:], kwargs
            return None, args, kwargs

        if args and callable(args[-1]):
            return args[-1], args[:-1], kwargs
        return None, args, kwargs

    def _dispatch_callback(self, callback_id: str, *args, **kwargs):
        debug_enabled = _is_callback_debug_enabled()
        start_time = None
        if debug_enabled:
            start_time = time.perf_counter()
            _log_callback_debug(
                self._logger,
                "DISPATCH_RECV",
                callback_id=callback_id,
                payload=_summarize_callback_payload(args, kwargs),
            )
        try:
            result = self._callback_registry.invoke(callback_id, *args, **kwargs)
            if debug_enabled:
                elapsed_ms = (time.perf_counter() - start_time) * 1000
                _log_callback_debug(
                    self._logger,
                    "DISPATCH_DONE",
                    callback_id=callback_id,
                    cost_ms=f"{elapsed_ms:.2f}",
                    result=_summarize_callback_value(result),
                )
            return result
        except Exception as exc:
            if debug_enabled:
                elapsed_ms = (time.perf_counter() - start_time) * 1000
                _log_callback_debug(
                    self._logger,
                    "DISPATCH_ERROR",
                    callback_id=callback_id,
                    cost_ms=f"{elapsed_ms:.2f}",
                    error=type(exc).__name__,
                    message=str(exc)[:200],
                )
            raise

    def _dispatch_trader_event(self, binding_id: str, event_name: str, *args, **kwargs):
        debug_enabled = _is_callback_debug_enabled()
        start_time = None
        if debug_enabled:
            start_time = time.perf_counter()
            _log_callback_debug(
                self._logger,
                "DISPATCH_RECV",
                callback_id=binding_id,
                event=event_name,
                payload=_summarize_callback_payload(args, kwargs),
            )
        try:
            result = self._callback_registry.invoke_event(binding_id, event_name, *args, **kwargs)
            if debug_enabled:
                elapsed_ms = (time.perf_counter() - start_time) * 1000
                _log_callback_debug(
                    self._logger,
                    "DISPATCH_DONE",
                    callback_id=binding_id,
                    event=event_name,
                    cost_ms=f"{elapsed_ms:.2f}",
                    result=_summarize_callback_value(result),
                )
            return result
        except Exception as exc:
            if debug_enabled:
                elapsed_ms = (time.perf_counter() - start_time) * 1000
                _log_callback_debug(
                    self._logger,
                    "DISPATCH_ERROR",
                    callback_id=binding_id,
                    event=event_name,
                    cost_ms=f"{elapsed_ms:.2f}",
                    error=type(exc).__name__,
                    message=str(exc)[:200],
                )
            raise

    def _call_xtdata_subscribe(self, method_name: str, args, kwargs):
        callback, args_wo_cb, kwargs_wo_cb = self._extract_callback(args, kwargs)
        if callback is None:
            module = self._xtdata._ensure_module()
            return getattr(module, method_name)(*args, **kwargs)

        self._ensure_connected()
        callback_id = self._callback_registry.register(
            callback,
            kind="xtdata_subscription",
            metadata={"method_name": method_name},
            prefix="xtdata_cb",
        )

        start_time = time.perf_counter()
        args_str = self._xtdata._summarize_args(args_wo_cb, kwargs_wo_cb)
        self._logger.info(f"[CALL] xtdata.{method_name}({args_str}, callback=<bridge>)")

        try:
            server_seq = self._conn.root.subscribe_xtdata_bridge(
                method_name,
                args_wo_cb,
                kwargs_wo_cb,
                callback_id,
                self._dispatch_callback,
            )
            public_id = server_seq if method_name in FORMULA_SUBSCRIBE_METHODS else self._next_seq()
            with self._subscription_lock:
                self._subscriptions[public_id] = SubscriptionState(
                    client_seq=public_id,
                    server_seq=server_seq,
                    method_name=method_name,
                    args=tuple(args_wo_cb),
                    kwargs=dict(kwargs_wo_cb),
                    callback_id=callback_id,
                )
            elapsed_ms = (time.perf_counter() - start_time) * 1000
            self._logger.info(f"[OK] xtdata.{method_name} | {elapsed_ms:.2f}ms | seq={public_id}")
            return public_id
        except Exception:
            self._callback_registry.unregister(callback_id)
            raise

    def _call_xtdata_unsubscribe(self, func_name: str, func, args, kwargs):
        seq = kwargs.get("seq")
        if seq is None and args:
            seq = args[0]
        if seq is None:
            return func(*args, **kwargs)

        with self._subscription_lock:
            state = self._subscriptions.get(seq)
        if state is None:
            return func(*args, **kwargs)

        start_time = time.perf_counter()
        self._logger.info(f"[CALL] xtdata.{func_name}(seq={seq})")
        result = self._conn.root.unsubscribe_xtdata_bridge(state.server_seq)
        self._callback_registry.unregister(state.callback_id)
        with self._subscription_lock:
            self._subscriptions.pop(seq, None)
        elapsed_ms = (time.perf_counter() - start_time) * 1000
        self._logger.info(f"[OK] xtdata.{func_name} | {elapsed_ms:.2f}ms | seq={seq}")
        return result

    def _translate_formula_request(self, args, kwargs):
        request_id = kwargs.get("request_id")
        if request_id is None and args:
            request_id = args[0]
        if request_id is None:
            return args, kwargs

        with self._subscription_lock:
            state = self._subscriptions.get(request_id)
        if state is None or state.method_name not in FORMULA_SUBSCRIBE_METHODS:
            return args, kwargs

        translated_args = list(args)
        translated_kwargs = dict(kwargs)
        if translated_args:
            translated_args[0] = state.server_seq
        else:
            translated_kwargs["request_id"] = state.server_seq
        return tuple(translated_args), translated_kwargs

    def _call_trader_register_callback(self, trader_module: RemoteModule, args, kwargs):
        callback_obj, _, _ = self._extract_callback(args, kwargs, object_mode=True)
        if callback_obj is None:
            raise CallbackError("register_callback 需要回调对象")

        callback_id = self._callback_registry.register(
            callback_obj,
            kind="xttrader_callback",
            prefix="trader_cb",
        )

        remote = trader_module._ensure_module()
        try:
            _log_callback_debug(
                self._logger,
                "REGISTER",
                callback_id=callback_id,
                kind="xttrader_callback",
                handler=callback_obj.__class__.__name__,
                session_id=getattr(trader_module._trader_state, "session_id", None),
            )
            result = remote.register_callback_bridge(callback_id, self._dispatch_trader_event)
            if trader_module._trader_state is not None:
                trader_module._trader_state.callback_binding_id = callback_id
            return result
        except Exception:
            self._callback_registry.unregister(callback_id)
            raise

    def _call_trader_async(self, trader_module: RemoteModule, method_name: str, args, kwargs):
        callback, args_wo_cb, kwargs_wo_cb = self._extract_callback(args, kwargs)
        if callback is None:
            remote = trader_module._ensure_module()
            return getattr(remote, method_name)(*args, **kwargs)

        callback_id = self._callback_registry.register(
            callback,
            kind="xttrader_async",
            one_shot=True,
            metadata={"method_name": method_name},
            prefix="trader_async",
        )

        remote = trader_module._ensure_module()
        try:
            if _is_callback_debug_enabled():
                _log_callback_debug(
                    self._logger,
                    "ASYNC_REGISTER",
                    callback_id=callback_id,
                    kind="xttrader_async",
                    method=method_name,
                    payload=_summarize_callback_payload(tuple(args_wo_cb), kwargs_wo_cb),
                    session_id=getattr(trader_module._trader_state, "session_id", None),
                )
            return remote.invoke_async_bridge(
                method_name,
                args_wo_cb,
                kwargs_wo_cb,
                callback_id,
                self._dispatch_callback,
            )
        except Exception:
            self._callback_registry.unregister(callback_id)
            raise

    def _restore_subscriptions_after_reconnect(self):
        with self._subscription_lock:
            active_subscriptions = list(self._subscriptions.values())

        for state in active_subscriptions:
            if not state.active:
                continue
            server_seq = self._conn.root.subscribe_xtdata_bridge(
                state.method_name,
                state.args,
                state.kwargs,
                state.callback_id,
                self._dispatch_callback,
            )
            state.server_seq = server_seq

    def _restore_traders_after_reconnect(self):
        for state in self._trader_states:
            remote = self._conn.root.create_trader(state.userdata_path, state.session_id)
            state.module._module = remote
            if state.callback_binding_id:
                _log_callback_debug(
                    self._logger,
                    "RESTORE_REGISTER_CALLBACK",
                    callback_id=state.callback_binding_id,
                    session_id=state.session_id,
                )
                remote.register_callback_bridge(state.callback_binding_id, self._dispatch_trader_event)
            if state.started:
                remote.start(*state.start_args, **state.start_kwargs)
            if state.connected:
                remote.connect(*state.connect_args, **state.connect_kwargs)
            for sub_args, sub_kwargs in state.subscriptions:
                remote.subscribe(*sub_args, **sub_kwargs)

    # ==================== 公共接口 ====================

    @property
    def xtdata(self):
        return self._xtdata

    @property
    def xttrader(self):
        return self._xttrader

    @property
    def xttype(self):
        return self._xttype

    @property
    def xtconstant(self):
        return self._xtconstant

    @property
    def xtview(self):
        return self._xtview

    def create_trader(self, userdata_path: str = None, session_id: int = None):
        self._ensure_connected()
        remote_trader = self._conn.root.create_trader(userdata_path, session_id)
        trader_module = RemoteModule(self, 'xttrader', remote_trader)
        trader_state = TraderModuleState(
            userdata_path=getattr(remote_trader, "userdata_path", userdata_path),
            session_id=getattr(remote_trader, "session_id", session_id),
            module=trader_module,
        )
        trader_module._trader_state = trader_state
        self._trader_states.append(trader_state)
        return trader_module

    def get_all_stocks(self):
        self._ensure_connected()
        return self._conn.root.get_all_stocks()

    def get_index_list(self):
        self._ensure_connected()
        return self._conn.root.get_index_list()

    def download_history_data2(self, stock_list: list, period: str = "1d",
                               start_time: str = "", end_time: str = "", incrementally: bool = None):
        self._ensure_connected()
        return self._conn.root.download_history_data2(stock_list, period, start_time, end_time, incrementally)

    def is_connected(self):
        return self._connected

    def get_service_status(self):
        self._ensure_connected()
        return self._conn.root.get_service_status()

    def reconnect(self):
        self._reconnect()

    def close(self):
        self._stop_heartbeat_thread()
        if self._bg_thread:
            self._bg_thread.stop()
            self._bg_thread = None
        self._connected = False
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
        self._conn = None
        self._logger.info("连接已关闭")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __repr__(self):
        status = "已连接" if self._connected else "已断开"
        ssl_status = "SSL" if self._use_ssl else "明文"
        return f"<XtQuantRemote {self._host}:{self._port} [{status}] [{ssl_status}]>"


# ==================== 全局便捷函数 ====================

_global_client = None


def connect(host=None, port=None, **kwargs):
    """创建全局连接。"""
    global _global_client
    if host is None:
        host = os.environ.get("XQSHARE_REMOTE_HOST", "localhost")
    if port is None:
        port = int(os.environ.get("XQSHARE_REMOTE_PORT", "18812"))
    _global_client = XtQuantRemote(host, port, **kwargs)
    return _global_client


def disconnect():
    """断开全局连接"""
    global _global_client
    if _global_client:
        _global_client.close()
        _global_client = None


def get_client():
    """获取全局客户端"""
    return _global_client


class _ModuleProxy:
    def __init__(self, name):
        self._name = name

    def __getattr__(self, attr):
        if _global_client is None:
            raise RuntimeError("请先调用 connect() 建立连接")
        return getattr(getattr(_global_client, self._name), attr)


xtdata = _ModuleProxy('xtdata')
xttrader = _ModuleProxy('xttrader')
xttype = _ModuleProxy('xttype')
xtview = _ModuleProxy('xtview')

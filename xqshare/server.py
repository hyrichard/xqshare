"""
XtQuant Share (xqshare) Server - Run on Windows to provide xtquant proxy service
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
from typing import Any, Dict, Optional

import rpyc
from rpyc.utils.server import ThreadedServer

from . import __version__ as XQSHARE_VERSION
from .auth import (
    AccountLevel,
    PermissionError,
    get_permission_checker,
)
from .paper_trader import PAPER_TRADER_MODE_ENV, PaperSimulator, PaperEvent, is_paper_trader_mode

# Import xtquant (only available on Windows)
try:
    import xtquant.xtconstant as xtconstant
    import xtquant.xtdata as xtdata
    import xtquant.xttrader as xttrader
    import xtquant.xttype as xttype
    from xtquant.xttrader import XtQuantTrader
    try:
        from xtquant.xttrader import XtQuantTraderCallback as XtQuantTraderCallbackBase
    except ImportError:
        class XtQuantTraderCallbackBase:  # type: ignore[no-redef]
            pass
    XTQUANT_AVAILABLE = True
except ImportError:
    XTQUANT_AVAILABLE = False
    xtdata = None
    xttrader = None
    xttype = None
    xtconstant = None
    XtQuantTrader = None

    class XtQuantTraderCallbackBase:  # type: ignore[no-redef]
        pass

try:
    import xtquant.xtview as xtview
    XTVIEW_AVAILABLE = True
except ImportError:
    xtview = None
    XTVIEW_AVAILABLE = False


# ==================== 日志配置 ====================

def setup_logging(log_dir: str = None, log_level: str = "INFO"):
    """配置日志系统"""
    if log_dir is None:
        log_dir = os.environ.get("XQSHARE_LOG_DIR", "logs")
    os.makedirs(log_dir, exist_ok=True)

    formatter = logging.Formatter(
        fmt='%(asctime)s.%(msecs)03d | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))

    if not any(isinstance(h, logging.StreamHandler) for h in root_logger.handlers):
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.INFO)
        root_logger.addHandler(console_handler)

    service_log = os.path.join(log_dir, f"xtquant_service_{datetime.now().strftime('%Y%m%d')}.log")
    if not any(isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", "") == os.path.abspath(service_log)
               for h in root_logger.handlers):
        file_handler = logging.FileHandler(service_log, encoding='utf-8')
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.DEBUG)
        root_logger.addHandler(file_handler)

    api_log = os.path.join(log_dir, f"api_calls_{datetime.now().strftime('%Y%m%d')}.log")
    api_logger = logging.getLogger('api')
    if not any(isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", "") == os.path.abspath(api_log)
               for h in api_logger.handlers):
        api_handler = logging.FileHandler(api_log, encoding='utf-8')
        api_handler.setFormatter(formatter)
        api_logger.addHandler(api_handler)
    api_logger.setLevel(logging.DEBUG)

    return logging.getLogger(__name__)


logger = None
api_logger = None
XTDATA_UNSUBSCRIBE_METHODS = {
    "subscribe_formula": "unsubscribe_formula",
}


def _init_logging(log_level="INFO"):
    global logger, api_logger
    logger = setup_logging(log_level=log_level)
    api_logger = logging.getLogger('api')


def _is_callback_debug_enabled() -> bool:
    """默认开启 callback 调试日志。"""
    return True


def _get_logger() -> logging.Logger:
    """获取可用的服务端 logger。"""
    return logger or logging.getLogger(__name__)


def _summarize_callback_value(value: Any, max_len: int = 120) -> str:
    """生成回调参数摘要，避免大对象刷屏。"""
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


def _summarize_callback_payload(args: tuple, kwargs: Dict[str, Any]) -> str:
    """生成回调入参摘要。"""
    parts = []
    if args:
        parts.append(f"args={_summarize_callback_value(args if len(args) != 1 else args[0])}")
    if kwargs:
        parts.append(f"kwargs={_summarize_callback_value(kwargs)}")
    return " ".join(parts) if parts else "no_payload"


def _log_callback_debug(phase: str, **fields: Any) -> None:
    """输出统一格式的 callback 调试日志。"""

    payload = {
        "thread": threading.current_thread().name,
        **fields,
    }
    message = " ".join(f"{key}={value}" for key, value in payload.items() if value is not None)
    api_logger.info(f"[CB][SERVER][{phase}] {message}")


# ==================== 日志装饰器 ====================

def _log_call(name: str, client_info: str, func, *args, **kwargs):
    """通用的 API 调用日志记录函数"""
    try:
        args_str = str(args)[:200] if args else ""
        kwargs_str = str(kwargs)[:200] if kwargs else ""
    except Exception:
        args_str = "<unserializable>"
        kwargs_str = ""

    api_logger.info(f"[CALL] {name} | client={client_info} | args={args_str} | kwargs={kwargs_str}")

    start_time = time.perf_counter()
    try:
        result = func(*args, **kwargs)
        elapsed_ms = (time.perf_counter() - start_time) * 1000
        result_summary = _summarize_result(result)
        api_logger.info(f"[OK] {name} | elapsed={elapsed_ms:.2f}ms | result={result_summary}")
        return result
    except Exception as e:
        elapsed_ms = (time.perf_counter() - start_time) * 1000
        api_logger.error(f"[ERROR] {name} | elapsed={elapsed_ms:.2f}ms | error={type(e).__name__}: {str(e)[:200]}")
        raise


def log_api_call(func_name: str = None):
    """记录 API 调用的装饰器"""
    def decorator(func):
        def wrapper(self, *args, **kwargs):
            name = func_name or func.__name__
            client_info = getattr(self, '_client_info', 'unknown')
            return _log_call(name, client_info, func, self, *args, **kwargs)
        return wrapper
    return decorator


def _summarize_result(result: Any, max_len: int = 200) -> str:
    """生成返回值摘要"""
    try:
        if result is None:
            return "None"
        if isinstance(result, (int, float, bool, str)):
            s = str(result)
            return s if len(s) <= max_len else s[:max_len] + "..."
        if isinstance(result, (list, tuple)):
            return f"{type(result).__name__}[len={len(result)}]"
        if isinstance(result, dict):
            keys = list(result.keys())[:5]
            return f"dict{{{', '.join(map(str, keys))}{'...' if len(result) > 5 else ''}}}"
        if hasattr(result, '__class__'):
            return f"<{result.__class__.__module__}.{result.__class__.__name__}>"
        return str(type(result))
    except Exception:
        return "<unserializable>"


# ==================== 异常定义 ====================

class AuthError(Exception):
    """认证错误"""


# ==================== 序列化传输优化 ====================

SERIALIZED_MARKER = "__xqshare_serialized__"


def _serialize_for_transfer(result):
    """将结果序列化以优化 RPyC 传输性能"""
    if result is None:
        return {SERIALIZED_MARKER: "none", "data": None}

    try:
        import pandas as pd
        if isinstance(result, pd.DataFrame):
            return {SERIALIZED_MARKER: "dataframe_csv", "data": result.to_csv(index=True)}
    except ImportError:
        pass

    if isinstance(result, dict):
        try:
            import pandas as pd

            def has_dataframe_recursive(obj):
                if isinstance(obj, pd.DataFrame):
                    return True
                if isinstance(obj, dict):
                    return any(has_dataframe_recursive(v) for v in obj.values())
                if isinstance(obj, (list, tuple)):
                    return any(has_dataframe_recursive(item) for item in obj)
                return False

            def serialize_dataframes(obj):
                if isinstance(obj, pd.DataFrame):
                    return {"__df__": True, "csv": obj.to_csv(index=True)}
                if isinstance(obj, dict):
                    return {k: serialize_dataframes(v) for k, v in obj.items()}
                if isinstance(obj, (list, tuple)):
                    return [serialize_dataframes(item) for item in obj]
                return obj

            if has_dataframe_recursive(result):
                serialized_dict = serialize_dataframes(result)
                json_str = json.dumps(serialized_dict, ensure_ascii=False, default=str)
                return {SERIALIZED_MARKER: "dict_with_dataframe", "data": json_str}
        except ImportError:
            pass

        try:
            json_str = json.dumps(result, ensure_ascii=False, default=str)
            return {SERIALIZED_MARKER: "json", "data": json_str}
        except (TypeError, ValueError):
            pass

    if isinstance(result, (list, tuple)):
        try:
            json_str = json.dumps(result, ensure_ascii=False, default=str)
            return {SERIALIZED_MARKER: "json", "data": json_str}
        except (TypeError, ValueError):
            pass

    return result


def _check_permission(permission_checker, account_level, method_name: str, args=(), kwargs=None):
    if permission_checker is None or account_level is None:
        return None
    return permission_checker.check_api_permission(account_level, method_name, args, kwargs or {})


def _get_xtdata_unsubscribe_method(method_name: str) -> str:
    """根据订阅方法名返回对应的取消订阅方法。"""
    return XTDATA_UNSUBSCRIBE_METHODS.get(method_name, "unsubscribe_quote")


# ==================== 回调桥接状态 ====================

@dataclass
class CallbackInfo:
    binding_id: str
    dispatcher: Any
    kind: str
    client_info: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    one_shot: bool = False
    registered_at: float = field(default_factory=time.time)
    call_count: int = 0


class CallbackManager:
    """管理客户端注册的回调绑定。"""

    def __init__(self):
        self._callbacks: Dict[str, CallbackInfo] = {}
        self._lock = threading.RLock()

    def register(self, binding_id: str, dispatcher, kind: str, client_info: str,
                 metadata: Optional[Dict[str, Any]] = None, one_shot: bool = False):
        with self._lock:
            self._callbacks[binding_id] = CallbackInfo(
                binding_id=binding_id,
                dispatcher=dispatcher,
                kind=kind,
                client_info=client_info,
                metadata=metadata or {},
                one_shot=one_shot,
            )
        _log_callback_debug(
            "REGISTER",
            callback_id=binding_id,
            kind=kind,
            client=client_info,
            one_shot=one_shot,
            metadata=_summarize_callback_value(metadata or {}),
        )

    def unregister(self, binding_id: str):
        with self._lock:
            info = self._callbacks.pop(binding_id, None)
        _log_callback_debug(
            "UNREGISTER",
            callback_id=binding_id,
            kind=getattr(info, "kind", None),
            client=getattr(info, "client_info", None),
        )

    def invoke(self, binding_id: str, *args, **kwargs):
        with self._lock:
            info = self._callbacks.get(binding_id)
        if info is None:
            return False

        info.call_count += 1
        start_time = time.perf_counter()
        _log_callback_debug(
            "FORWARD_START",
            callback_id=binding_id,
            kind=info.kind,
            client=info.client_info,
            payload=_summarize_callback_payload(args, kwargs),
        )
        try:
            result = info.dispatcher(binding_id, *args, **kwargs)
            elapsed_ms = (time.perf_counter() - start_time) * 1000
            _log_callback_debug(
                "FORWARD_DONE",
                callback_id=binding_id,
                kind=info.kind,
                client=info.client_info,
                cost_ms=f"{elapsed_ms:.2f}",
                result=_summarize_callback_value(result),
            )
            return result
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - start_time) * 1000
            _log_callback_debug(
                "FORWARD_ERROR",
                callback_id=binding_id,
                kind=info.kind,
                client=info.client_info,
                cost_ms=f"{elapsed_ms:.2f}",
                error=type(exc).__name__,
                message=str(exc)[:200],
            )
            raise
        finally:
            if info.one_shot:
                self.unregister(binding_id)

    def invoke_event(self, binding_id: str, event_name: str, *args, **kwargs):
        with self._lock:
            info = self._callbacks.get(binding_id)
        if info is None:
            return False

        info.call_count += 1
        start_time = time.perf_counter()
        _log_callback_debug(
            "FORWARD_START",
            callback_id=binding_id,
            kind=info.kind,
            client=info.client_info,
            event=event_name,
            payload=_summarize_callback_payload(args, kwargs),
        )
        try:
            result = info.dispatcher(binding_id, event_name, *args, **kwargs)
            elapsed_ms = (time.perf_counter() - start_time) * 1000
            _log_callback_debug(
                "FORWARD_DONE",
                callback_id=binding_id,
                kind=info.kind,
                client=info.client_info,
                event=event_name,
                cost_ms=f"{elapsed_ms:.2f}",
                result=_summarize_callback_value(result),
            )
            return result
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - start_time) * 1000
            _log_callback_debug(
                "FORWARD_ERROR",
                callback_id=binding_id,
                kind=info.kind,
                client=info.client_info,
                event=event_name,
                cost_ms=f"{elapsed_ms:.2f}",
                error=type(exc).__name__,
                message=str(exc)[:200],
            )
            raise

    def list_callbacks(self):
        with self._lock:
            return {
                binding_id: {
                    "kind": info.kind,
                    "client": info.client_info,
                    "registered_at": info.registered_at,
                    "call_count": info.call_count,
                    "metadata": dict(info.metadata),
                }
                for binding_id, info in self._callbacks.items()
            }

    def clear_client_callbacks(self, client_info: str):
        with self._lock:
            to_remove = [binding_id for binding_id, info in self._callbacks.items() if info.client_info == client_info]
            for binding_id in to_remove:
                self._callbacks.pop(binding_id, None)
        return to_remove


class TraderCallbackAdapter(XtQuantTraderCallbackBase):
    """将 xttrader 交易回调显式转发到客户端。

    优先显式实现常见交易推送入口，尽量贴近 xtquant 官方示例和本地工作形态；
    同时保留对未显式覆盖 `on_*` 的兜底转发，避免回调面收缩。
    """

    def __init__(self, binding_id: str, callback_manager: CallbackManager):
        self._binding_id = binding_id
        self._callback_manager = callback_manager

    def _forward_event(self, event_name: str, *args, **kwargs):
        """统一记录并转发交易事件。"""
        _log_callback_debug(
            "EVENT_RECV",
            callback_id=self._binding_id,
            event=event_name,
            payload=_summarize_callback_payload(args, kwargs),
        )
        return self._callback_manager.invoke_event(self._binding_id, event_name, *args, **kwargs)

    def on_disconnected(self):
        return self._forward_event("on_disconnected")

    def on_connected(self):
        return self._forward_event("on_connected")

    def on_account_status(self, status):
        return self._forward_event("on_account_status", status)

    def on_stock_asset(self, asset):
        return self._forward_event("on_stock_asset", asset)

    def on_stock_order(self, order):
        return self._forward_event("on_stock_order", order)

    def on_stock_trade(self, trade):
        return self._forward_event("on_stock_trade", trade)

    def on_stock_position(self, position):
        return self._forward_event("on_stock_position", position)

    def on_order_error(self, order_error):
        return self._forward_event("on_order_error", order_error)

    def on_cancel_error(self, cancel_error):
        return self._forward_event("on_cancel_error", cancel_error)

    def on_order_stock_async_response(self, response):
        return self._forward_event("on_order_stock_async_response", response)

    def on_cancel_order_stock_async_response(self, response):
        return self._forward_event("on_cancel_order_stock_async_response", response)

    def on_smt_appointment_async_response(self, response):
        return self._forward_event("on_smt_appointment_async_response", response)

    def on_bank_transfer_async_response(self, response):
        return self._forward_event("on_bank_transfer_async_response", response)

    def __getattr__(self, name):
        """兜底转发未显式声明的 `on_*` 交易回调。"""
        if name.startswith("on_"):
            def handler(*args, **kwargs):
                _log_callback_debug(
                    "EVENT_FALLBACK",
                    callback_id=self._binding_id,
                    event=name,
                    payload=_summarize_callback_payload(args, kwargs),
                )
                return self._forward_event(name, *args, **kwargs)
            return handler
        raise AttributeError(name)


# ==================== 模块代理（带日志和权限检查） ====================

class LoggingProxy:
    """通用代理：拦截模块/对象的方法调用并记录日志，支持递归包装返回对象和权限检查"""

    def __init__(self, target, target_name: str, client_info_getter, permission_checker=None, account_level=None):
        object.__setattr__(self, '_target', target)
        object.__setattr__(self, '_target_name', target_name)
        object.__setattr__(self, '_get_client_info', client_info_getter)
        object.__setattr__(self, '_permission_checker', permission_checker)
        object.__setattr__(self, '_account_level', account_level)

    def __getattr__(self, name):
        target = object.__getattribute__(self, '_target')
        target_name = object.__getattribute__(self, '_target_name')
        get_client_info = object.__getattribute__(self, '_get_client_info')
        permission_checker = object.__getattribute__(self, '_permission_checker')
        account_level = object.__getattribute__(self, '_account_level')

        attr = getattr(target, name)

        if callable(attr):
            def wrapper(*args, **kwargs):
                full_name = f"{target_name}.{name}"
                error = _check_permission(permission_checker, account_level, full_name, args, kwargs)
                if error:
                    api_logger.warning(f"[权限拒绝] {full_name} | client={get_client_info()} | {error}")
                    raise error

                result = _log_call(full_name, get_client_info(), attr, *args, **kwargs)

                if result is not None and hasattr(result, '__class__'):
                    if not isinstance(result, (int, float, str, bool, list, dict, tuple, type(None), bytes)):
                        if not result.__class__.__module__.startswith('builtins'):
                            return LoggingProxy(result, full_name, get_client_info, permission_checker, account_level)

                if isinstance(result, list):
                    wrapped_list = []
                    has_complex_obj = False
                    for item in result:
                        if item is not None and hasattr(item, '__class__'):
                            if not isinstance(item, (int, float, str, bool, dict, tuple, type(None), bytes)):
                                if not item.__class__.__module__.startswith('builtins'):
                                    wrapped_list.append(LoggingProxy(item, full_name, get_client_info, permission_checker, account_level))
                                    has_complex_obj = True
                                    continue
                        wrapped_list.append(item)
                    if has_complex_obj:
                        return wrapped_list

                return _serialize_for_transfer(result)

            wrapper.__name__ = name
            return wrapper

        return attr

    def __setattr__(self, name, value):
        return setattr(object.__getattribute__(self, '_target'), name, value)

    def __dir__(self):
        return dir(object.__getattribute__(self, '_target'))

    def __repr__(self):
        return repr(object.__getattribute__(self, '_target'))


LoggingModuleProxy = LoggingProxy


class TraderBridge:
    """交易对象桥接层：保留原 API，同时接管 callback 场景。

    在 paper 模式下，交易操作委托给 PaperSimulator，
    回调事件仍通过 CallbackManager 统一派发，
    确保模拟路径与真实柜台路径走同一套回调链路。
    """

    def __init__(self, trader, userdata_path: Optional[str], session_id: Optional[int],
                 client_info_getter, permission_checker, account_level,
                 callback_manager: CallbackManager,
                 paper_simulator: Optional[PaperSimulator] = None):
        self._trader = trader
        self._client_info_getter = client_info_getter
        self._permission_checker = permission_checker
        self._account_level = account_level
        self._callback_manager = callback_manager
        self._proxy = LoggingProxy(trader, 'xttrader', client_info_getter, permission_checker, account_level)
        self._callback_binding_id = None
        self._callback_adapter = None
        self.userdata_path = userdata_path
        self.session_id = session_id

        # 纸面交易相关状态
        self._paper = paper_simulator
        self._paper_ticker_thread: Optional[threading.Thread] = None
        self._paper_stop_event = threading.Event()
        self._paper_active_orders: dict[int, tuple[str, str]] = {}  # order_id -> account_key
        self._paper_lock = threading.Lock()

    def _check(self, method_name: str, args=(), kwargs=None):
        error = _check_permission(self._permission_checker, self._account_level, method_name, args, kwargs)
        if error:
            api_logger.warning(f"[权限拒绝] {method_name} | client={self._client_info_getter()} | {error}")
            raise error

    @property
    def is_paper_mode(self) -> bool:
        """是否为纸面交易模式。"""
        return self._paper is not None

    def _emit_paper_events(self, events: list) -> None:
        """通过 CallbackManager 派发纸面交易事件，与真实柜台回调走同一条链路。"""
        if not events or self._callback_adapter is None:
            return
        for event in events:
            handler = getattr(self._callback_adapter, event.event_name, None)
            if callable(handler):
                try:
                    handler(event.data)
                except Exception:
                    logger.exception(
                        "[纸面交易] 回调派发异常 | event=%s | session_id=%s",
                        event.event_name,
                        self.session_id,
                    )

    def _start_paper_ticker(self) -> None:
        """启动纸面交易撮合推进线程。"""
        if self._paper_ticker_thread is not None and self._paper_ticker_thread.is_alive():
            return
        self._paper_stop_event.clear()
        self._paper_ticker_thread = threading.Thread(
            target=self._paper_ticker_loop,
            daemon=True,
            name=f"paper-ticker-{self.session_id}",
        )
        self._paper_ticker_thread.start()
        logger.info("[纸面交易] 撮合线程已启动 | session_id=%s", self.session_id)

    def _stop_paper_ticker(self) -> None:
        """停止纸面交易撮合推进线程。"""
        self._paper_stop_event.set()
        if self._paper is not None:
            self._paper.stop()
        if self._paper_ticker_thread is not None:
            self._paper_ticker_thread.join(timeout=2)
            self._paper_ticker_thread = None

    def _paper_ticker_loop(self) -> None:
        """后台循环：推进所有活跃订单的撮合。"""
        while not self._paper_stop_event.is_set():
            try:
                self._paper_ticker_tick()
            except Exception:
                logger.exception("[纸面交易] 撮合循环异常 | session_id=%s", self.session_id)
            self._paper_stop_event.wait(self._paper.fill_interval_seconds if self._paper else 0.05)

    def _paper_ticker_tick(self) -> None:
        """扫描所有活跃账户的活跃订单并推进一次 tick。"""
        if self._paper is None:
            return
        # 收集所有活跃订单
        with self._paper_lock:
            order_tasks = list(self._paper_active_orders.items())
        finished = []
        for order_id, account_key in order_tasks:
            events = self._paper.tick(account_key, order_id)
            if events is not None:
                self._emit_paper_events(events)
            # 检查是否仍在活跃
            if not self._paper.is_order_active(account_key, order_id):
                finished.append(order_id)
        if finished:
            with self._paper_lock:
                for order_id in finished:
                    self._paper_active_orders.pop(order_id, None)

    def register_callback_bridge(self, binding_id: str, dispatcher):
        self._check("xttrader.register_callback")
        logger.info(
            "[回调桥] 开始注册交易回调 | client=%s | session_id=%s | binding_id=%s",
            self._client_info_getter(),
            self.session_id,
            binding_id,
        )
        self._callback_manager.register(
            binding_id,
            dispatcher,
            kind="xttrader_callback",
            client_info=self._client_info_getter(),
            metadata={"bridge": "register_callback"},
        )
        self._callback_binding_id = binding_id
        self._callback_adapter = TraderCallbackAdapter(binding_id, self._callback_manager)
        _log_callback_debug(
            "REGISTER",
            callback_id=binding_id,
            kind="xttrader_callback",
            client=self._client_info_getter(),
            session_id=self.session_id,
        )
        # 纸面交易模式不需要向真实 trader 注册回调
        if self.is_paper_mode:
            logger.info(
                "[回调桥] 交易回调注册完成(paper) | client=%s | session_id=%s | binding_id=%s",
                self._client_info_getter(),
                self.session_id,
                binding_id,
            )
            return 0
        if hasattr(self._trader, "register_callback"):
            result = self._trader.register_callback(self._callback_adapter)
            logger.info(
                "[回调桥] 交易回调注册完成 | client=%s | session_id=%s | binding_id=%s | result=%s",
                self._client_info_getter(),
                self.session_id,
                binding_id,
                _summarize_result(result),
            )
            return result
        logger.info(
            "[回调桥] 交易回调注册完成 | client=%s | session_id=%s | binding_id=%s | result=None",
            self._client_info_getter(),
            self.session_id,
            binding_id,
        )
        return None

    def invoke_async_bridge(self, method_name: str, args, kwargs, callback_id: str, dispatcher):
        self._check(f"xttrader.{method_name}", tuple(args), dict(kwargs))
        logger.info(
            "[异步桥] 开始发起交易异步调用 | client=%s | session_id=%s | method=%s | callback_id=%s",
            self._client_info_getter(),
            self.session_id,
            method_name,
            callback_id,
        )
        self._callback_manager.register(
            callback_id,
            dispatcher,
            kind="xttrader_async",
            client_info=self._client_info_getter(),
            metadata={"method_name": method_name},
            one_shot=True,
        )
        _log_callback_debug(
            "ASYNC_REGISTER",
            callback_id=callback_id,
            kind="xttrader_async",
            client=self._client_info_getter(),
            method=method_name,
            session_id=self.session_id,
            payload=_summarize_callback_payload(tuple(args), dict(kwargs)) if True else None,
        )

        def on_result(*cb_args, **cb_kwargs):
            _log_callback_debug(
                "ASYNC_RESULT_RECV",
                callback_id=callback_id,
                kind="xttrader_async",
                client=self._client_info_getter(),
                method=method_name,
                payload=_summarize_callback_payload(cb_args, cb_kwargs),
            )
            return self._callback_manager.invoke(callback_id, *cb_args, **cb_kwargs)

        method = getattr(self._trader, method_name)
        call_kwargs = dict(kwargs)
        call_kwargs["callback"] = on_result
        result = _log_call(f"xttrader.{method_name}", self._client_info_getter(), method, *args, **call_kwargs)
        logger.info(
            "[异步桥] 交易异步调用已提交 | client=%s | session_id=%s | method=%s | callback_id=%s | result=%s",
            self._client_info_getter(),
            self.session_id,
            method_name,
            callback_id,
            _summarize_result(result),
        )
        return result

    def __getattr__(self, name):
        # 纸面交易模式下拦截交易操作
        if self.is_paper_mode:
            handler = self._get_paper_method(name)
            if handler is not None:
                return handler
        return getattr(self._proxy, name)

    def _get_paper_method(self, name: str):
        """返回纸面交易模式下对应方法的处理器，无匹配则返回 None。"""
        paper_methods = {
            "start": self._paper_start,
            "stop": self._paper_stop,
            "connect": self._paper_connect,
            "disconnect": self._paper_disconnect,
            "subscribe": self._paper_subscribe,
            "unsubscribe": self._paper_unsubscribe,
            "subscribe_account": self._paper_subscribe,
            "unsubscribe_account": self._paper_unsubscribe,
            "register_callback": self._paper_register_callback,
            "set_callback": self._paper_register_callback,
            "order_stock": self._paper_order_stock,
            "order_stock_async": self._paper_order_stock_async,
            "cancel_order_stock": self._paper_cancel_order_stock,
            "cancel_order_stock_async": self._paper_cancel_order_stock_async,
            "cancel_order_stock_sysid": self._paper_cancel_order_stock_sysid,
            "cancel_order_stock_sysid_async": self._paper_cancel_order_stock_sysid_async,
            "query_stock_asset": self._paper_query_stock_asset,
            "query_stock_position": self._paper_query_stock_position,
            "query_stock_positions": self._paper_query_stock_positions,
            "query_stock_order": self._paper_query_stock_order,
            "query_stock_orders": self._paper_query_stock_orders,
            "query_stock_trades": self._paper_query_stock_trades,
            "query_account_status": self._paper_query_account_status,
            "query_account_infos": self._paper_query_account_infos,
            "query_account_status_async": self._paper_query_account_status_async,
            "query_account_infos_async": self._paper_query_account_infos_async,
            "query_stock_asset_async": self._paper_query_stock_asset_async,
            "query_stock_positions_async": self._paper_query_stock_positions_async,
            "query_stock_orders_async": self._paper_query_stock_orders_async,
            "query_stock_trades_async": self._paper_query_stock_trades_async,
            "query_credit_detail": self._paper_unsupported_dict,
            "query_credit_detail_async": self._paper_unsupported_dict,
            "query_stk_compacts": self._paper_unsupported,
            "query_credit_subjects": self._paper_unsupported,
            "query_credit_slo_code": self._paper_unsupported,
            "query_credit_assure": self._paper_unsupported,
            "query_new_purchase_limit": self._paper_unsupported_dict,
        }
        return paper_methods.get(name)

    def _paper_register_callback(self, callback=None) -> int:
        """纸面交易 register_callback：空操作。

        回调已在 register_callback_bridge 阶段通过 CallbackManager 桥接完成，
        这里只兼容本地模式直接调用 register_callback 的场景。
        """
        logger.info("[纸面交易] register_callback | session_id=%s", self.session_id)
        return 0

    def _paper_start(self, *args, **kwargs) -> int:
        """纸面交易 start：标记会话启动。"""
        logger.info("[纸面交易] start | session_id=%s", self.session_id)
        return 0

    def _paper_stop(self) -> None:
        """纸面交易 stop：停止撮合线程。"""
        self._stop_paper_ticker()
        logger.info("[纸面交易] stop | session_id=%s", self.session_id)

    def _paper_connect(self) -> int:
        """纸面交易 connect：模拟连接成功，推送 on_connected。"""
        if self._callback_adapter is not None:
            self._callback_adapter.on_connected()
        logger.info("[纸面交易] connect | session_id=%s", self.session_id)
        return 0

    def _paper_disconnect(self) -> None:
        """纸面交易 disconnect：推送 on_disconnected。"""
        if self._callback_adapter is not None:
            self._callback_adapter.on_disconnected()
        self._stop_paper_ticker()
        logger.info("[纸面交易] disconnect | session_id=%s", self.session_id)

    def _paper_subscribe(self, account, *args, **kwargs) -> int:
        """纸面交易 subscribe：初始化账户状态并推送事件。"""
        events = self._paper.subscribe_account(account)
        self._emit_paper_events(events)
        logger.info("[纸面交易] subscribe | session_id=%s", self.session_id)
        return 0

    def _paper_unsubscribe(self, account, *args, **kwargs) -> int:
        """纸面交易 unsubscribe。"""
        self._paper.unsubscribe_account(account)
        return 0

    def _paper_order_stock(self, account, stock_code, order_type, order_volume,
                           price_type, price, strategy_name="", order_remark="") -> int:
        """纸面交易下单：委托给 PaperSimulator，派发回调，启动撮合。"""
        order_id, events = self._paper.order_stock(
            account, stock_code, order_type, order_volume,
            price_type, price, strategy_name, order_remark,
        )
        self._emit_paper_events(events)
        # 如果订单被接受（非废单），注册到活跃订单并启动撮合
        if self._paper.is_order_active(self._paper._get_account_key(account), order_id):
            with self._paper_lock:
                self._paper_active_orders[order_id] = self._paper._get_account_key(account)
            self._start_paper_ticker()
        return order_id

    def _paper_order_stock_async(self, account, stock_code, order_type, order_volume,
                                  price_type, price, strategy_name="", order_remark="",
                                  callback=None) -> int:
        """纸面交易异步下单。"""
        from .paper_trader import PaperOrderResponse
        order_id = self._paper_order_stock(
            account, stock_code, order_type, order_volume,
            price_type, price, strategy_name, order_remark,
        )
        if callable(callback):
            order = self._paper.query_stock_order(account, order_id)
            asset = self._paper.query_stock_asset(account)
            response = PaperOrderResponse(
                account_type=asset.account_type,
                account_id=asset.account_id,
                order_id=order_id,
                strategy_name=strategy_name,
                order_remark=order_remark,
                error_msg="" if (order and order.order_status != 57) else ("废单" if order else "未知错误"),
                seq=order_id,
                order_sysid=order.order_sysid if order else "",
            )
            callback(response)
        return order_id

    def _paper_cancel_order_stock(self, account, order_id) -> int:
        """纸面交易撤单。

        模拟真实柜台：撤单请求经过同步校验，如果柜台当场拒绝（订单不存在/已终态），
        拒绝事件 on_cancel_error 在 RPC 返回前同步推送，模拟嵌套在 RPC 调用内的回调。
        可撤订单由 ticker 异步推进并通过 on_cancel_error / on_stock_order 到达。
        """
        cancel_result, _events = self._paper.cancel_order_stock(account, order_id)
        # 同步推送柜台当场拒绝的事件，模拟真实 cancel_order_stock 内部嵌套的回调推送
        sync_reject_events = self._paper.cancel_order_stock_sync_check(account, order_id)
        self._emit_paper_events(sync_reject_events)
        return cancel_result

    def _paper_cancel_order_stock_async(self, account, order_id, callback=None) -> int:
        """纸面交易异步撤单。"""
        from .paper_trader import PaperCancelOrderResponse
        cancel_result = self._paper_cancel_order_stock(account, order_id)
        if callable(callback):
            account_state = self._paper._get_or_create_account_state(account)[0]
            order = self._paper.query_stock_order(account, order_id)
            callback(PaperCancelOrderResponse(
                account_type=account_state.account_type,
                account_id=account_state.account_id,
                cancel_result=cancel_result,
                order_id=order_id,
                order_sysid=order.order_sysid if order else "",
                seq=order_id,
                error_msg="" if cancel_result == 0 else "撤单失败",
            ))
        return cancel_result

    def _paper_cancel_order_stock_sysid(self, account, market, sysid) -> int:
        """纸面交易按系统编号撤单。

        与 _paper_cancel_order_stock 一致，同步推送柜台当场拒绝的事件。
        """
        cancel_result, _events = self._paper.cancel_order_stock_sysid(account, market, sysid)
        sync_reject_events = self._paper.cancel_order_stock_sysid_sync_check(account, market, sysid)
        self._emit_paper_events(sync_reject_events)
        return cancel_result

    def _paper_cancel_order_stock_sysid_async(self, account, market, sysid, callback=None) -> int:
        """纸面交易按系统编号异步撤单。"""
        from .paper_trader import PaperCancelOrderResponse
        cancel_result = self._paper_cancel_order_stock_sysid(account, market, sysid)
        if callable(callback):
            account_state = self._paper._get_or_create_account_state(account)[0]
            callback(PaperCancelOrderResponse(
                account_type=account_state.account_type,
                account_id=account_state.account_id,
                cancel_result=cancel_result,
                order_id=0,
                order_sysid=str(sysid),
                seq=0,
                error_msg="" if cancel_result == 0 else "撤单失败",
            ))
        return cancel_result

    def _paper_query_stock_asset(self, account):
        """纸面交易查询资金。"""
        return self._paper.query_stock_asset(account)

    def _paper_query_stock_position(self, account, stock_code):
        """纸面交易查询单只持仓。"""
        return self._paper.query_stock_position(account, stock_code)

    def _paper_query_stock_positions(self, account):
        """纸面交易查询所有持仓。"""
        return self._paper.query_stock_positions(account)

    def _paper_query_stock_order(self, account, order_id):
        """纸面交易查询单笔委托。"""
        return self._paper.query_stock_order(account, order_id)

    def _paper_query_stock_orders(self, account, cancelable_only=False):
        """纸面交易查询委托列表。"""
        return self._paper.query_stock_orders(account, cancelable_only)

    def _paper_query_stock_trades(self, account):
        """纸面交易查询成交列表。"""
        return self._paper.query_stock_trades(account)

    def _paper_query_account_status(self):
        """纸面交易查询账户状态。"""
        return self._paper.query_account_status()

    def _paper_query_account_infos(self):
        """纸面交易查询账户信息。"""
        return self._paper.query_account_infos()

    def _paper_query_account_status_async(self, callback=None) -> int:
        """纸面交易异步查询账户状态。"""
        if callable(callback):
            callback(self._paper.query_account_status())
        return 0

    def _paper_query_account_infos_async(self, callback=None) -> int:
        """纸面交易异步查询账户信息。"""
        if callable(callback):
            callback(self._paper.query_account_infos())
        return 0

    def _paper_query_stock_asset_async(self, account, callback=None) -> int:
        """纸面交易异步查询资金。"""
        if callable(callback):
            callback(self._paper.query_stock_asset(account))
        return 0

    def _paper_query_stock_positions_async(self, account, callback=None) -> int:
        """纸面交易异步查询持仓。"""
        if callable(callback):
            callback(self._paper.query_stock_positions(account))
        return 0

    def _paper_query_stock_orders_async(self, account, cancelable_only=False, callback=None) -> int:
        """纸面交易异步查询委托。"""
        if callable(callback):
            callback(self._paper.query_stock_orders(account, cancelable_only))
        return 0

    def _paper_query_stock_trades_async(self, account, callback=None) -> int:
        """纸面交易异步查询成交。"""
        if callable(callback):
            callback(self._paper.query_stock_trades(account))
        return 0

    def _paper_unsupported(self, *args, **kwargs):
        """纸面交易不支持的接口，默认返回空列表。

        query_credit_detail 和 query_new_purchase_limit 返回空字典，
        因为真实柜台返回 dict 结构，空列表会破坏调用方的索引访问。
        """
        return []

    def _paper_unsupported_dict(self, *args, **kwargs):
        """纸面交易不支持且返回类型应为 dict 的接口。"""
        return {}


# ==================== 服务类 ====================

class XtQuantService(rpyc.Service):
    """完全透明代理服务"""

    _xtdata = xtdata
    _xttrader = xttrader
    _xttype = xttype
    _xtconstant = xtconstant
    _xtview = xtview
    _permission_checker = None
    _callback_manager = CallbackManager()
    _xtdata_subscriptions: Dict[Any, Dict[str, Any]] = {}

    def on_connect(self, conn):
        self._conn = conn
        self._authenticated = False
        self._client_id = None
        self._account_level = AccountLevel.FREE
        try:
            if hasattr(conn, 'peer'):
                self._client_info = f"{conn.peer}"
            elif hasattr(conn, '_channel') and hasattr(conn._channel, 'stream'):
                stream = conn._channel.stream
                if hasattr(stream, 'sock'):
                    peer = stream.sock.getpeername()
                    self._client_info = f"{peer[0]}:{peer[1]}"
                else:
                    self._client_info = "unknown"
            else:
                self._client_info = "unknown"
        except Exception:
            self._client_info = "unknown"
        logger.info(f"[连接] 客户端接入: {self._client_info}")
        logger.info(f"[连接] 会话初始化完成: client={self._client_info}")

    def on_disconnect(self, conn):
        client_info = getattr(self, '_client_info', 'unknown')
        logger.info(f"[断开] 开始清理客户端会话: {client_info}")
        to_remove = [
            server_seq for server_seq, info in list(XtQuantService._xtdata_subscriptions.items())
            if info.get("client_info") == client_info
        ]
        for server_seq in to_remove:
            info = XtQuantService._xtdata_subscriptions.pop(server_seq, None)
            if info is None:
                continue
            try:
                unsubscribe = getattr(self._xtdata, info.get("unsubscribe_method", "unsubscribe_quote"))
                unsubscribe(server_seq)
            except Exception:
                pass
            callback_id = info.get("callback_id")
            if callback_id:
                XtQuantService._callback_manager.unregister(callback_id)

        XtQuantService._callback_manager.clear_client_callbacks(client_info)
        logger.info(f"[断开] 客户端离开: {client_info}")

    def _delayed_disconnect(self, delay: float = 0.5):
        def _close():
            try:
                self._conn.close()
            except Exception:
                pass
        threading.Timer(delay, _close).start()

    def _require_auth(self):
        if not self._authenticated:
            logger.warning(f"[未授权] 未认证的访问尝试: {self._client_info}")
            self._delayed_disconnect()
            raise AuthError("未授权访问，请先认证")

    @log_api_call("authenticate")
    def exposed_authenticate(self, client_id, client_secret):
        checker = XtQuantService._permission_checker
        checker.check_and_reload_if_changed()

        valid, account_level = checker.verify_secret(client_id, client_secret)
        if not valid:
            logger.warning(f"[认证失败] client_id={client_id}")
            self._delayed_disconnect()
            raise AuthError("认证失败：无效的客户端凭证")

        self._authenticated = True
        self._client_id = client_id
        self._account_level = account_level
        self._client_info = f"{client_id}@{self._client_info}"
        logger.info(f"[认证成功] client_id={client_id} | level={account_level.value}")
        return {"success": True, "level": account_level.value}

    @log_api_call("heartbeat")
    def exposed_heartbeat(self):
        return "pong"

    @log_api_call("get_xtdata")
    def exposed_get_xtdata(self):
        self._require_auth()
        return LoggingModuleProxy(
            self._xtdata, 'xtdata',
            lambda: self._client_info,
            XtQuantService._permission_checker,
            self._account_level
        )

    @log_api_call("get_xttrader")
    def exposed_get_xttrader(self):
        self._require_auth()
        return LoggingModuleProxy(
            self._xttrader, 'xttrader',
            lambda: self._client_info,
            XtQuantService._permission_checker,
            self._account_level
        )

    @log_api_call("get_xttype")
    def exposed_get_xttype(self):
        self._require_auth()
        return self._xttype

    def exposed_get_xtconstant(self):
        self._require_auth()
        return self._xtconstant

    @log_api_call("get_xtview")
    def exposed_get_xtview(self):
        self._require_auth()
        if self._xtview is None:
            raise RuntimeError("xtview 模块不可用，请检查 xtquant 版本是否支持")
        return LoggingModuleProxy(
            self._xtview, 'xtview',
            lambda: self._client_info,
            XtQuantService._permission_checker,
            self._account_level
        )

    @log_api_call("create_trader")
    def exposed_create_trader(self, userdata_path: str = None, session_id: int = None):
        self._require_auth()
        error = _check_permission(XtQuantService._permission_checker, self._account_level, "create_xttrader")
        if error:
            logger.warning(f"[权限拒绝] create_xttrader | client={self._client_info} | {error}")
            raise error

        if session_id is None:
            session_id = int(time.time())

        trader_mode = os.environ.get(PAPER_TRADER_MODE_ENV, "real").strip().lower()
        paper_simulator = None
        if trader_mode == "paper":
            paper_simulator = PaperSimulator.from_env(
                xtdata_module=self._xtdata,
                xtconstant_module=self._xtconstant,
            )
            # 纸面交易模式下仍需要一个 trader 对象作为 LoggingProxy 的目标，
            # 但不会真正调用它的交易方法，所以用 stub 占位。
            trader = type("PaperTraderStub", (), {
                "userdata_path": userdata_path,
                "session_id": session_id,
            })()
            logger.info(
                "[创建Trader] mode=paper | userdata_path=%s | session_id=%s",
                userdata_path,
                session_id,
            )
        else:
            if not XTQUANT_AVAILABLE:
                raise RuntimeError("xtquant 库未安装")

            if userdata_path is None:
                userdata_path = os.environ.get("QMT_USERDATA_PATH")
            if userdata_path is None:
                raise ValueError("必须提供 userdata_path 参数或设置 QMT_USERDATA_PATH 环境变量")

            trader = XtQuantTrader(userdata_path, session_id)
            logger.info(f"[创建Trader] mode=real | userdata_path={userdata_path} | session_id={session_id}")
        logger.info(f"[创建Trader] 交易对象就绪 | client={self._client_info} | session_id={session_id}")
        return TraderBridge(
            trader,
            userdata_path,
            session_id,
            lambda: self._client_info,
            XtQuantService._permission_checker,
            self._account_level,
            XtQuantService._callback_manager,
            paper_simulator=paper_simulator,
        )

    @log_api_call("subscribe_xtdata_bridge")
    def exposed_subscribe_xtdata_bridge(self, method_name: str, args: tuple, kwargs: dict,
                                        callback_id: str, dispatcher):
        self._require_auth()
        logger.info(
            "[行情桥] 开始订阅 | client=%s | method=%s | callback_id=%s",
            self._client_info,
            method_name,
            callback_id,
        )

        full_name = f"xtdata.{method_name}"
        permission_kwargs = dict(kwargs)
        permission_kwargs["callback"] = "<bridge>"
        error = _check_permission(XtQuantService._permission_checker, self._account_level, full_name, args, permission_kwargs)
        if error:
            logger.warning(f"[权限拒绝] {full_name} | client={self._client_info} | {error}")
            raise error

        XtQuantService._callback_manager.register(
            callback_id,
            dispatcher,
            kind="xtdata_subscription",
            client_info=self._client_info,
            metadata={"method_name": method_name},
        )

        try:
            target = getattr(self._xtdata, method_name)

            def on_data(*cb_args, **cb_kwargs):
                return XtQuantService._callback_manager.invoke(callback_id, *cb_args, **cb_kwargs)

            call_kwargs = dict(kwargs)
            call_kwargs["callback"] = on_data
            server_seq = target(*args, **call_kwargs)
            XtQuantService._xtdata_subscriptions[server_seq] = {
                "client_info": self._client_info,
                "callback_id": callback_id,
                "method_name": method_name,
                "unsubscribe_method": _get_xtdata_unsubscribe_method(method_name),
            }
            logger.info(
                "[行情桥] 订阅完成 | client=%s | method=%s | callback_id=%s | server_seq=%s",
                self._client_info,
                method_name,
                callback_id,
                server_seq,
            )
            return server_seq
        except Exception:
            XtQuantService._callback_manager.unregister(callback_id)
            raise

    @log_api_call("unsubscribe_xtdata_bridge")
    def exposed_unsubscribe_xtdata_bridge(self, server_seq):
        self._require_auth()
        logger.info(
            "[行情桥] 开始退订 | client=%s | server_seq=%s",
            self._client_info,
            server_seq,
        )
        info = XtQuantService._xtdata_subscriptions.get(server_seq)
        if info is None:
            raise ValueError(f"未找到订阅: {server_seq}")
        if info.get("client_info") != self._client_info:
            raise AuthError("无权取消其他客户端的订阅")

        unsubscribe_method = info.get("unsubscribe_method", "unsubscribe_quote")
        callback_id = info.get("callback_id")

        result = getattr(self._xtdata, unsubscribe_method)(server_seq)
        if callback_id:
            XtQuantService._callback_manager.unregister(callback_id)
        XtQuantService._xtdata_subscriptions.pop(server_seq, None)
        logger.info(
            "[行情桥] 退订完成 | client=%s | server_seq=%s | callback_id=%s",
            self._client_info,
            server_seq,
            callback_id,
        )
        return result

    @log_api_call("download_history_data2")
    def exposed_download_history_data2(self, stock_list: list, period: str = "1d",
                                       start_time: str = "", end_time: str = "", incrementally: bool = None):
        status = {'finished': 0, 'total': 0, 'done': False, 'result': {}, 'message': ''}

        def on_progress(data):
            status['finished'] = data.get('finished', 0)
            status['total'] = data.get('total', 0)
            status['done'] = status['finished'] >= status['total']
            status['message'] = data.get('message', '')
            if 'result' in data:
                import datetime as dt
                from xtquant import xtbson as bson
                region_result = bson.BSON.decode(data.get('result'))
                for stock, info in region_result.items():
                    info['start_time'] = str(dt.datetime.fromtimestamp(info.get('start_time') / 1000))
                    info['end_time'] = str(dt.datetime.fromtimestamp(info.get('end_time') / 1000))
                    status['result'][stock] = info

        self._xtdata.download_history_data2(
            stock_list, period, start_time, end_time,
            callback=on_progress, incrementally=incrementally
        )
        return status

    @log_api_call("get_all_stocks")
    def exposed_get_all_stocks(self):
        self._require_auth()
        return self._xtdata.get_stock_list_in_sector("沪深A股")

    @log_api_call("get_index_list")
    def exposed_get_index_list(self):
        self._require_auth()
        return self._xtdata.get_stock_list_in_sector("沪深指数")

    @log_api_call("get_service_status")
    def exposed_get_service_status(self):
        self._require_auth()
        return {
            "uptime": time.time() - getattr(self, '_start_time', time.time()),
            "client_id": self._client_id,
            "active_callbacks": len(XtQuantService._callback_manager.list_callbacks()),
            "active_subscriptions": len(XtQuantService._xtdata_subscriptions),
        }

    @log_api_call("ping")
    def exposed_ping(self):
        return "pong"

    @log_api_call("test_async_callback")
    def exposed_test_async_callback(self, callback_func, delay: float = 2.0, count: int = 5):
        self._require_auth()
        error = _check_permission(XtQuantService._permission_checker, self._account_level, "test_async_callback")
        if error:
            logger.warning(f"[权限拒绝] test_async_callback | client={self._client_info} | {error}")
            raise error

        def async_call():
            for i in range(count):
                time.sleep(delay)
                try:
                    result = callback_func(f"异步回调 #{i+1}/{count}，时间: {time.strftime('%H:%M:%S')}")
                    api_logger.info(f"[异步回调] #{i+1} 执行成功，返回: {result}")
                except Exception as e:
                    api_logger.error(f"[异步回调] #{i+1} 执行失败: {e}")

        thread = threading.Thread(target=async_call, daemon=True)
        thread.start()
        return f"已启动异步回调，共 {count} 次，间隔 {delay} 秒"


# ==================== 服务启动 ====================

def create_ssl_context(certfile=None, keyfile=None):
    if not certfile or not keyfile:
        return None
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(certfile, keyfile)
    return ctx


def start_server(host="0.0.0.0", port=None, use_ssl=False, certfile=None, keyfile=None, log_level="INFO", env_file=None):
    """启动服务"""
    try:
        from dotenv import load_dotenv
        load_dotenv(env_file)
    except ImportError:
        pass

    if port is None:
        port = int(os.environ.get("XQSHARE_PORT", "18812"))

    if not XTQUANT_AVAILABLE:
        print("错误: xtquant 库未安装，请先安装 xtquant")
        return

    _init_logging(log_level)
    XtQuantService._start_time = time.time()

    print("=" * 70)
    print("  XtQuant Share (xqshare) 服务")
    print(f"  版本号: {XQSHARE_VERSION}")
    print("=" * 70)
    print(f"  监听地址: {host}:{port}")
    print(f"  SSL 加密: {'启用' if use_ssl else '禁用'}")
    print(f"  日志级别: {log_level}")
    print(f"  交易后端: {'纸面交易' if is_paper_trader_mode() else '真实柜台'}")
    print("  Callback调试: 启用")
    print("=" * 70)

    if XtQuantService._permission_checker is None:
        XtQuantService._permission_checker = get_permission_checker()

    logger.info(
        f"服务启动 | version={XQSHARE_VERSION} | host={host} | port={port} | ssl={use_ssl} | "
        f"env_file={env_file!r}"
    )

    config = {
        'allow_public_attrs': True,
        'allow_pickle': True,
        'allow_getattr': True,
        'allow_setattr': True,
        'allow_delattr': True,
        'allow_all_attrs': True,
        'sync_request_timeout': 300,
    }

    ssl_context = None
    if use_ssl:
        ssl_context = create_ssl_context(certfile, keyfile)
        if ssl_context:
            logger.info("SSL 证书加载成功")
            print("  ✓ SSL 证书加载成功")
        else:
            logger.warning("SSL 证书加载失败")
            print("  ⚠ SSL 证书加载失败")

    server_kwargs = {
        'hostname': host,
        'port': port,
        'protocol_config': config,
    }

    try:
        server = ThreadedServer(XtQuantService, ssl_context=ssl_context, **server_kwargs)
    except TypeError:
        if ssl_context:
            class SSLThreadedServer(ThreadedServer):
                def _accept_method(self, sock):
                    try:
                        return ssl_context.wrap_socket(sock, server_side=True)
                    except Exception as e:
                        logger.error(f"SSL 包装失败: {e}")
                        raise

            server = SSLThreadedServer(XtQuantService, **server_kwargs)
            logger.info("使用兼容模式启动 SSL")
        else:
            server = ThreadedServer(XtQuantService, **server_kwargs)

    print("\n  服务已启动，等待客户端连接...")
    print("  按 Ctrl+C 停止服务\n")

    try:
        server.start()
    except KeyboardInterrupt:
        logger.info("服务停止（用户中断）")
        print("\n  服务已停止")
        server.close()
    except Exception as e:
        logger.error(f"服务异常: {e}")
        raise


def main():
    """命令行入口函数"""
    import argparse

    parser = argparse.ArgumentParser(
        description="XtQuant Share (xqshare) 服务",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  xqshare-server                    # 使用默认配置启动
  xqshare-server --port 18813       # 指定端口
  xqshare-server --ssl --cert cert.pem --key key.pem  # 启用 SSL

环境变量:
  XQSHARE_PORT      服务端口 (默认: 18812)
  QMT_USERDATA_PATH QMT userdata_mini 目录路径
        """
    )
    parser.add_argument("--host", default="0.0.0.0", help="监听地址 (默认: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=None, help="监听端口 (默认: 18812 或 XQSHARE_PORT)")
    parser.add_argument("--ssl", action="store_true", help="启用 SSL 加密")
    parser.add_argument("--cert", help="SSL 证书文件")
    parser.add_argument("--key", help="SSL 私钥文件")
    parser.add_argument("--log-level", default="INFO", help="日志级别 (默认: INFO)")
    parser.add_argument("--env-file", default=".env", help="环境变量文件 (默认: .env)")

    args = parser.parse_args()

    start_server(
        host=args.host,
        port=args.port,
        use_ssl=args.ssl,
        certfile=args.cert,
        keyfile=args.key,
        log_level=args.log_level,
        env_file=args.env_file
    )


if __name__ == "__main__":
    main()

"""纸面交易实现。

本模块模拟 xttrader 的股票交易侧能力，保留真实 xtdata 行情读取，
用于非交易时段或柜台不可用时做流程验证。

当前版本只重点支持股票买入/卖出、部分成交、撤单竞态，以及资产/
持仓/委托/成交查询。其余更复杂的信用、期货和专项交易接口暂不展开。
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Mapping, Optional

LOGGER = logging.getLogger(__name__)

PAPER_TRADER_MODE_ENV = "XQSHARE_TRADER_MODE"
PAPER_TRADER_SEED_ENV = "XQSHARE_PAPER_TRADER_SEED"
PAPER_TRADER_FILL_INTERVAL_ENV = "XQSHARE_PAPER_TRADER_FILL_INTERVAL_SECONDS"

DEFAULT_FILL_INTERVAL_SECONDS = 0.05
DEFAULT_ORDER_ERROR_ID = 1001
DEFAULT_CANCEL_ERROR_ID = 2001

ACCOUNT_TYPE_NAME_TO_VALUE = {
    "FUTURE": 1,
    "STOCK": 2,
    "CREDIT": 3,
    "FUTURE_OPTION": 5,
    "STOCK_OPTION": 6,
    "HUGANGTONG": 7,
    "INCOME_SWAP": 8,
    "NEW3BOARD": 10,
    "SHENGANGTONG": 11,
}


def _as_str(value: Any, default: str = "") -> str:
    """尽量把对象值转换成字符串。"""

    if value in (None, ""):
        return default
    return str(value)


def _as_int(value: Any, default: int = 0) -> int:
    """尽量把对象值转换成整数。"""

    if value in (None, ""):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_float(value: Any, default: float = 0.0) -> float:
    """尽量把对象值转换成浮点数。"""

    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _resolve_constant(module: Any | None, name: str, default: int) -> int:
    """从 xtconstant 模块中读取常量，缺失时回退到默认值。"""

    if module is None:
        return default

    value = getattr(module, name, default)
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _resolve_account_type(account_type: Any) -> Any:
    """把账户类型归一化成更稳定的口径。"""

    if isinstance(account_type, int):
        return account_type
    normalized = _as_str(account_type).strip().upper()
    return ACCOUNT_TYPE_NAME_TO_VALUE.get(normalized, normalized or "STOCK")


def _normalize_stock_code(stock_code: Any) -> str:
    """把证券代码归一化成大写字符串。"""

    return _as_str(stock_code).strip().upper()


def _now_text() -> str:
    """返回当前时间字符串，便于模拟柜台时间字段。"""

    return datetime.now().strftime("%Y%m%d %H:%M:%S")


def is_paper_trader_mode() -> bool:
    """判断当前是否启用纸面交易模式。"""

    return _as_str(os.environ.get(PAPER_TRADER_MODE_ENV, "real")).strip().lower() == "paper"


@dataclass(frozen=True)
class PaperTraderConstants:
    """纸面交易所需的最小常量集合。"""

    stock_buy: int = 23
    stock_sell: int = 24
    fix_price: int = 11
    latest_price: int = 5
    market_peer_price_first: int = 44
    market_sz_instbusi_restcancel: int = 46
    market_sz_full_or_cancel: int = 48
    order_reported: int = 50
    order_reported_cancel: int = 51
    order_partsucc_cancel: int = 52
    order_part_cancel: int = 53
    order_canceled: int = 54
    order_part_succ: int = 55
    order_succeeded: int = 56
    order_junk: int = 57
    account_status_ok: int = 0
    account_status_closed: int = 6

    @classmethod
    def from_module(cls, module: Any | None) -> "PaperTraderConstants":
        """从真实 xtconstant 模块或 mock 中提取常量。"""

        return cls(
            stock_buy=_resolve_constant(module, "STOCK_BUY", 23),
            stock_sell=_resolve_constant(module, "STOCK_SELL", 24),
            fix_price=_resolve_constant(module, "FIX_PRICE", 11),
            latest_price=_resolve_constant(module, "LATEST_PRICE", 5),
            market_peer_price_first=_resolve_constant(module, "MARKET_PEER_PRICE_FIRST", 44),
            market_sz_instbusi_restcancel=_resolve_constant(module, "MARKET_SZ_INSTBUSI_RESTCANCEL", 46),
            market_sz_full_or_cancel=_resolve_constant(module, "MARKET_SZ_FULL_OR_CANCEL", 48),
            order_reported=_resolve_constant(module, "ORDER_REPORTED", 50),
            order_reported_cancel=_resolve_constant(module, "ORDER_REPORTED_CANCEL", 51),
            order_partsucc_cancel=_resolve_constant(module, "ORDER_PARTSUCC_CANCEL", 52),
            order_part_cancel=_resolve_constant(module, "ORDER_PART_CANCEL", 53),
            order_canceled=_resolve_constant(module, "ORDER_CANCELED", 54),
            order_part_succ=_resolve_constant(module, "ORDER_PART_SUCC", 55),
            order_succeeded=_resolve_constant(module, "ORDER_SUCCEEDED", 56),
            order_junk=_resolve_constant(module, "ORDER_JUNK", 57),
            account_status_ok=_resolve_constant(module, "ACCOUNT_STATUS_OK", 0),
            account_status_closed=_resolve_constant(module, "ACCOUNT_STATUS_CLOSED", 6),
        )

    @property
    def terminal_order_statuses(self) -> frozenset[int]:
        """返回终态订单状态集合。"""

        return frozenset(
            {
                self.order_part_cancel,
                self.order_canceled,
                self.order_succeeded,
                self.order_junk,
            }
        )

    @property
    def cancelable_order_statuses(self) -> frozenset[int]:
        """返回仍可继续撤单的状态集合。"""

        return frozenset(
            {
                self.order_reported,
                self.order_reported_cancel,
                self.order_partsucc_cancel,
                self.order_part_succ,
            }
        )

    @property
    def market_order_types(self) -> frozenset[int]:
        """返回当前纸面交易识别为“按行情成交”的报价类型集合。"""

        return frozenset(
            {
                self.latest_price,
                self.market_peer_price_first,
                self.market_sz_instbusi_restcancel,
                self.market_sz_full_or_cancel,
            }
        )


@dataclass(frozen=True)
class PaperSeedPosition:
    """纸面交易账户的初始持仓。"""

    stock_code: str
    volume: int
    avg_price: float = 0.0
    can_use_volume: int | None = None
    last_price: float | None = None
    yesterday_volume: int | None = None


@dataclass(frozen=True)
class PaperTraderSeed:
    """纸面交易账户初始化种子。"""

    cash: float = 1_000_000.0
    frozen_cash: float = 0.0
    positions: tuple[PaperSeedPosition, ...] = ()

    @classmethod
    def from_mapping(cls, raw_seed: Mapping[str, Any] | None) -> "PaperTraderSeed":
        """从字典结构构造纸面交易种子。"""

        if not raw_seed:
            return cls()

        cash = _as_float(raw_seed.get("cash", 1_000_000.0), 1_000_000.0)
        frozen_cash = _as_float(raw_seed.get("frozen_cash", 0.0), 0.0)
        raw_positions = raw_seed.get("positions", ())
        positions: list[PaperSeedPosition] = []

        if isinstance(raw_positions, Mapping):
            for stock_code, value in raw_positions.items():
                if isinstance(value, Mapping):
                    positions.append(
                        PaperSeedPosition(
                            stock_code=_normalize_stock_code(stock_code),
                            volume=_as_int(value.get("volume", 0)),
                            avg_price=_as_float(value.get("avg_price", 0.0)),
                            can_use_volume=(
                                _as_int(value.get("can_use_volume", 0))
                                if value.get("can_use_volume") is not None
                                else None
                            ),
                            last_price=(
                                _as_float(value.get("last_price", 0.0))
                                if value.get("last_price") is not None
                                else None
                            ),
                            yesterday_volume=(
                                _as_int(value.get("yesterday_volume", 0))
                                if value.get("yesterday_volume") is not None
                                else None
                            ),
                        )
                    )
                else:
                    positions.append(
                        PaperSeedPosition(
                            stock_code=_normalize_stock_code(stock_code),
                            volume=_as_int(value),
                        )
                    )
        elif isinstance(raw_positions, (list, tuple)):
            for item in raw_positions:
                if isinstance(item, Mapping):
                    stock_code = _normalize_stock_code(item.get("stock_code"))
                    if not stock_code:
                        continue
                    positions.append(
                        PaperSeedPosition(
                            stock_code=stock_code,
                            volume=_as_int(item.get("volume", 0)),
                            avg_price=_as_float(item.get("avg_price", 0.0)),
                            can_use_volume=(
                                _as_int(item.get("can_use_volume", 0))
                                if item.get("can_use_volume") is not None
                                else None
                            ),
                            last_price=(
                                _as_float(item.get("last_price", 0.0))
                                if item.get("last_price") is not None
                                else None
                            ),
                            yesterday_volume=(
                                _as_int(item.get("yesterday_volume", 0))
                                if item.get("yesterday_volume") is not None
                                else None
                            ),
                        )
                    )

        return cls(cash=cash, frozen_cash=frozen_cash, positions=tuple(positions))

    @classmethod
    def from_env(cls, env_value: str | None = None) -> "PaperTraderSeed":
        """从环境变量中的 JSON 字符串构造种子。"""

        if not env_value:
            return cls()
        payload = json.loads(env_value)
        if not isinstance(payload, Mapping):
            raise ValueError("XQSHARE_PAPER_TRADER_SEED 必须是 JSON 对象。")
        return cls.from_mapping(payload)


@dataclass
class PaperAccountStatus:
    """纸面交易的账户状态快照。"""

    account_type: Any
    account_id: str
    status: int


@dataclass
class PaperAccountInfo:
    """纸面交易的账户信息快照。"""

    account_type: Any
    account_id: str
    status: int
    cash: float
    frozen_cash: float
    market_value: float
    total_asset: float
    fetch_balance: float


@dataclass
class PaperAsset:
    """纸面交易的资金资产快照。"""

    account_type: Any
    account_id: str
    cash: float
    frozen_cash: float
    market_value: float
    total_asset: float
    fetch_balance: float


@dataclass
class PaperPosition:
    """纸面交易的持仓快照。"""

    account_type: Any
    account_id: str
    stock_code: str
    volume: int
    can_use_volume: int
    open_price: float
    market_value: float
    frozen_volume: int
    on_road_volume: int
    yesterday_volume: int
    avg_price: float
    direction: int
    last_price: float
    profit_rate: float
    secu_account: str
    instrument_name: str


@dataclass
class PaperOrder:
    """纸面交易的委托快照。"""

    account_type: Any
    account_id: str
    stock_code: str
    order_id: int
    order_sysid: str
    order_time: str
    order_type: int
    order_volume: int
    price_type: int
    price: float
    traded_volume: int
    traded_price: float
    order_status: int
    status_msg: str
    strategy_name: str
    order_remark: str
    direction: int
    offset_flag: int
    secu_account: str
    instrument_name: str


@dataclass
class PaperTrade:
    """纸面交易的成交快照。"""

    account_type: Any
    account_id: str
    stock_code: str
    order_type: int
    traded_id: str
    traded_time: str
    traded_price: float
    traded_volume: int
    traded_amount: float
    order_id: int
    order_sysid: str
    strategy_name: str
    order_remark: str
    direction: int
    offset_flag: int
    commission: float
    secu_account: str
    instrument_name: str


@dataclass
class PaperOrderError:
    """纸面交易的报单失败快照。"""

    account_type: Any
    account_id: str
    order_id: int
    error_id: int
    error_msg: str
    strategy_name: str
    order_remark: str
    order_status: int = 0


@dataclass
class PaperCancelError:
    """纸面交易的撤单失败快照。"""

    account_type: Any
    account_id: str
    order_id: int
    market: int
    order_sysid: str
    error_id: int
    error_msg: str
    order_status: int = 0


@dataclass
class PaperOrderResponse:
    """纸面交易的异步报单响应。"""

    account_type: Any
    account_id: str
    order_id: int
    strategy_name: str
    order_remark: str
    error_msg: str
    seq: int
    order_sysid: str


@dataclass
class PaperCancelOrderResponse:
    """纸面交易的异步撤单响应。"""

    account_type: Any
    account_id: str
    cancel_result: int
    order_id: int
    order_sysid: str
    seq: int
    error_msg: str


@dataclass
class _PositionState:
    """账户内的持仓内部状态。"""

    stock_code: str
    volume: int
    can_use_volume: int
    avg_price: float
    last_price: float = 0.0
    frozen_volume: int = 0
    on_road_volume: int = 0
    yesterday_volume: int = 0


@dataclass
class _OrderState:
    """账户内的委托内部状态。"""

    account_key: tuple[str, str]
    account_id: str
    account_type: Any
    stock_code: str
    order_id: int
    order_sysid: str
    order_time: str
    order_type: int
    order_volume: int
    price_type: int
    price: float
    strategy_name: str
    order_remark: str
    instrument_name: str
    order_status: int
    status_msg: str
    traded_volume: int = 0
    traded_price: float = 0.0
    traded_amount: float = 0.0
    direction: int = 0
    offset_flag: int = 0
    secu_account: str = ""
    cancel_requested: bool = False
    cancel_requested_at: float | None = None
    trade_count: int = 0
    accepted: bool = True
    reject_error_id: int = 0
    reject_error_msg: str = ""


@dataclass
class _TradeState:
    """账户内的成交内部状态。"""

    account_key: tuple[str, str]
    account_id: str
    account_type: Any
    stock_code: str
    order_type: int
    traded_id: str
    traded_time: str
    traded_price: float
    traded_volume: int
    traded_amount: float
    order_id: int
    order_sysid: str
    strategy_name: str
    order_remark: str
    direction: int
    offset_flag: int
    commission: float
    secu_account: str
    instrument_name: str


@dataclass
class _AccountState:
    """账户级内部状态。"""

    account_id: str
    account_type: Any
    cash: float
    frozen_cash: float
    positions: dict[str, _PositionState] = field(default_factory=dict)
    orders: dict[int, _OrderState] = field(default_factory=dict)
    trades: list[_TradeState] = field(default_factory=list)
    subscribed: bool = False
    status: int = 0


class PaperXtQuantTrader:
    """纸面交易版 XtQuantTrader。

    这个实现只模拟股票买卖链路，不访问真实柜台；行情与合约信息仍然
    由传入的 xtdata 模块提供。
    """

    def __init__(
        self,
        userdata_path: str | None,
        session_id: int | None,
        xtdata_module: Any = None,
        xtconstant_module: Any = None,
        seed: PaperTraderSeed | Mapping[str, Any] | None = None,
        fill_interval_seconds: float | None = None,
    ) -> None:
        """初始化纸面交易对象。"""

        self.userdata_path = userdata_path
        self.session_id = session_id
        self._xtdata = xtdata_module
        self._constants = PaperTraderConstants.from_module(xtconstant_module)
        self._seed = seed if isinstance(seed, PaperTraderSeed) else PaperTraderSeed.from_mapping(seed)
        self._fill_interval_seconds = (
            _as_float(fill_interval_seconds, DEFAULT_FILL_INTERVAL_SECONDS)
            if fill_interval_seconds is not None
            else _as_float(os.environ.get(PAPER_TRADER_FILL_INTERVAL_ENV), DEFAULT_FILL_INTERVAL_SECONDS)
        )

        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._callback: Any | None = None
        self._connected = False
        self._started = False
        self._timeout = 0.0
        self._relaxed_response_order_enabled: bool | None = None

        self._order_seq = 0
        self._async_seq = 0
        self._accounts: dict[tuple[str, str], _AccountState] = {}
        self._orders_by_sysid: dict[str, tuple[tuple[str, str], int]] = {}
        self._instrument_name_cache: dict[str, str] = {}

    @classmethod
    def from_env(
        cls,
        userdata_path: str | None,
        session_id: int | None,
        xtdata_module: Any = None,
        xtconstant_module: Any = None,
    ) -> "PaperXtQuantTrader":
        """从环境变量构造纸面交易对象。"""

        seed = PaperTraderSeed.from_env(os.environ.get(PAPER_TRADER_SEED_ENV))
        fill_interval = _as_float(
            os.environ.get(PAPER_TRADER_FILL_INTERVAL_ENV),
            DEFAULT_FILL_INTERVAL_SECONDS,
        )
        return cls(
            userdata_path=userdata_path,
            session_id=session_id,
            xtdata_module=xtdata_module,
            xtconstant_module=xtconstant_module,
            seed=seed,
            fill_interval_seconds=fill_interval,
        )

    def register_callback(self, callback: Any) -> int:
        """注册交易回调对象。"""

        with self._lock:
            self._callback = callback
        return 0

    def start(self) -> None:
        """启动纸面交易会话。"""

        with self._lock:
            self._started = True
            self._stop_event.clear()

    def stop(self) -> None:
        """停止纸面交易会话。"""

        callback = None
        should_emit = False
        with self._lock:
            should_emit = self._connected and self._callback is not None
            callback = self._callback
            self._connected = False
            self._started = False
            self._stop_event.set()
            for state in self._accounts.values():
                state.status = self._constants.account_status_closed

        if should_emit and callback is not None:
            self._safe_emit("on_disconnected")

    def connect(self) -> int:
        """连接纸面交易会话。"""

        with self._lock:
            self._connected = True

        self._safe_emit("on_connected")
        return 0

    def set_timeout(self, timeout: float = 0) -> None:
        """记录超时时间配置。"""

        with self._lock:
            self._timeout = _as_float(timeout, 0.0)

    def set_relaxed_response_order_enabled(self, enabled: bool) -> None:
        """兼容 QMT 的宽松响应顺序开关。"""

        with self._lock:
            self._relaxed_response_order_enabled = bool(enabled)

    def run_forever(self) -> None:
        """阻塞等待，直到调用 stop()。"""

        self._stop_event.wait()

    def sleep(self, seconds: float) -> None:
        """兼容 QMT 的异步睡眠接口。"""

        time.sleep(max(_as_float(seconds, 0.0), 0.0))

    def subscribe(self, account: Any) -> int:
        """订阅账户并推送初始资产/持仓。"""

        account_state, _ = self._get_or_create_account_state(account)
        with self._lock:
            account_state.subscribed = True
            account_state.status = self._constants.account_status_ok
            asset_snapshot = self._build_asset_snapshot_locked(account_state)
            position_snapshots = self._build_position_snapshots_locked(account_state)

        self._safe_emit("on_account_status", self._build_account_status_snapshot(account_state))
        self._safe_emit("on_stock_asset", asset_snapshot)
        for position_snapshot in position_snapshots:
            self._safe_emit("on_stock_position", position_snapshot)
        return 0

    def unsubscribe(self, account: Any) -> int:
        """取消账户订阅。"""

        account_state, _ = self._get_or_create_account_state(account)
        with self._lock:
            account_state.subscribed = False
        return 0

    def subscribe_account(self, account: Any) -> int:
        """兼容旧版本的订阅接口名。"""

        return self.subscribe(account)

    def unsubscribe_account(self, account: Any) -> int:
        """兼容旧版本的退订接口名。"""

        return self.unsubscribe(account)

    def query_account_infos(self) -> list[PaperAccountInfo]:
        """查询账户信息列表。"""

        with self._lock:
            return [self._build_account_info_snapshot(state) for state in self._accounts.values()]

    def query_account_infos_async(self, callback: Any = None) -> int:
        """异步查询账户信息列表。"""

        return self._schedule_async_result(callback, self.query_account_infos())

    def query_account_status(self) -> list[PaperAccountStatus]:
        """查询账户状态列表。"""

        with self._lock:
            return [self._build_account_status_snapshot(state) for state in self._accounts.values()]

    def query_account_status_async(self, callback: Any = None) -> int:
        """异步查询账户状态列表。"""

        return self._schedule_async_result(callback, self.query_account_status())

    def query_stock_asset(self, account: Any) -> PaperAsset:
        """查询账户资产。"""

        account_state, _ = self._get_or_create_account_state(account)
        with self._lock:
            return self._build_asset_snapshot_locked(account_state)

    def query_stock_asset_async(self, account: Any, callback: Any = None) -> int:
        """异步查询账户资产。"""

        return self._schedule_async_result(callback, self.query_stock_asset(account))

    def query_stock_position(self, account: Any, stock_code: str) -> PaperPosition | None:
        """查询单个持仓。"""

        account_state, _ = self._get_or_create_account_state(account)
        normalized_stock_code = _normalize_stock_code(stock_code)
        with self._lock:
            position_state = account_state.positions.get(normalized_stock_code)
            if position_state is None:
                return None
            return self._build_position_snapshot_locked(account_state, position_state)

    def query_stock_positions(self, account: Any) -> list[PaperPosition]:
        """查询账户全部持仓。"""

        account_state, _ = self._get_or_create_account_state(account)
        with self._lock:
            return self._build_position_snapshots_locked(account_state)

    def query_stock_positions_async(self, account: Any, callback: Any = None) -> int:
        """异步查询账户全部持仓。"""

        return self._schedule_async_result(callback, self.query_stock_positions(account))

    def query_stock_order(self, account: Any, order_id: int) -> PaperOrder | None:
        """查询单个委托。"""

        account_state, _ = self._get_or_create_account_state(account)
        normalized_order_id = _as_int(order_id, -1)
        with self._lock:
            order_state = account_state.orders.get(normalized_order_id)
            if order_state is None:
                return None
            return self._build_order_snapshot_locked(order_state)

    def query_stock_orders(self, account: Any, cancelable_only: bool = False) -> list[PaperOrder]:
        """查询账户当日委托。"""

        account_state, _ = self._get_or_create_account_state(account)
        with self._lock:
            order_states = list(account_state.orders.values())

        if cancelable_only:
            order_states = [
                state for state in order_states
                if state.order_status in self._constants.cancelable_order_statuses
            ]

        order_states.sort(key=lambda state: (state.order_time, state.order_id))
        with self._lock:
            return [self._build_order_snapshot_locked(state) for state in order_states]

    def query_stock_orders_async(
        self,
        account: Any,
        callback: Any = None,
        cancelable_only: bool = False,
    ) -> int:
        """异步查询账户当日委托。"""

        return self._schedule_async_result(callback, self.query_stock_orders(account, cancelable_only))

    def query_stock_trades(self, account: Any) -> list[PaperTrade]:
        """查询账户当日成交。"""

        account_state, _ = self._get_or_create_account_state(account)
        with self._lock:
            trade_states = sorted(
                account_state.trades,
                key=lambda state: (state.traded_time, state.traded_id),
            )
            return [self._build_trade_snapshot_locked(state) for state in trade_states]

    def query_stock_trades_async(self, account: Any, callback: Any = None) -> int:
        """异步查询账户当日成交。"""

        return self._schedule_async_result(callback, self.query_stock_trades(account))

    def order_stock(
        self,
        account: Any,
        stock_code: str,
        order_type: int,
        order_volume: int,
        price_type: int,
        price: float,
        strategy_name: str = "",
        order_remark: str = "",
    ) -> int:
        """提交股票买卖委托。"""

        normalized_stock_code = _normalize_stock_code(stock_code)
        resolved_order_type = _as_int(order_type)
        resolved_order_volume = _as_int(order_volume)
        resolved_price_type = _as_int(price_type)
        resolved_price = _as_float(price, 0.0)
        if resolved_order_volume <= 0:
            raise ValueError(f"order_volume 必须大于 0，传入: {order_volume!r}")
        if resolved_order_type not in (self._constants.stock_buy, self._constants.stock_sell):
            raise ValueError(f"当前纸面交易只支持 STOCK_BUY/STOCK_SELL，传入: {order_type!r}")

        account_state, account_key = self._get_or_create_account_state(account)
        execution_price = self._resolve_execution_price(normalized_stock_code, resolved_price_type, resolved_price)
        if execution_price <= 0:
            return self._reject_order(
                account_state=account_state,
                account_key=account_key,
                stock_code=normalized_stock_code,
                order_type=resolved_order_type,
                order_volume=resolved_order_volume,
                price_type=resolved_price_type,
                price=resolved_price,
                strategy_name=strategy_name,
                order_remark=order_remark,
                error_msg="无法解析有效报价，纸面委托被拒绝。",
            )

        rejection_payload: tuple[PaperOrder, PaperOrderError] | None = None
        order_id: int | None = None
        with self._lock:
            order_id = self._next_order_id_locked()
            order_sysid = self._build_order_sysid(order_id)
            instrument_name = self._resolve_instrument_name(normalized_stock_code)
            account_type = account_state.account_type
            can_accept = True
            position_state: _PositionState | None = None
            order_state = _OrderState(
                account_key=account_key,
                account_id=account_state.account_id,
                account_type=account_type,
                stock_code=normalized_stock_code,
                order_id=order_id,
                order_sysid=order_sysid,
                order_time=_now_text(),
                order_type=resolved_order_type,
                order_volume=resolved_order_volume,
                price_type=resolved_price_type,
                price=execution_price,
                strategy_name=_as_str(strategy_name),
                order_remark=_as_str(order_remark),
                instrument_name=instrument_name,
                order_status=self._constants.order_reported,
                status_msg="已报",
                direction=0,
                offset_flag=0,
                secu_account=account_state.account_id,
            )
            account_state.orders[order_id] = order_state
            self._orders_by_sysid[order_sysid] = (account_key, order_id)

            if resolved_order_type == self._constants.stock_buy:
                can_accept = self._check_buy_capacity_locked(account_state, resolved_order_volume, execution_price)
                if can_accept:
                    # 先冻结资金，再让后台线程分步撮合，避免并发委托把同一笔现金重复用掉。
                    self._reserve_buy_cash_locked(account_state, resolved_order_volume, execution_price)
                else:
                    rejection_payload = self._reject_existing_order_locked(
                        account_state=account_state,
                        order_state=order_state,
                        error_msg="可用资金不足，纸面买入委托被拒绝。",
                        error_id=DEFAULT_ORDER_ERROR_ID,
                    )
            else:
                position_state = account_state.positions.get(normalized_stock_code)
                available_volume = self._get_sellable_volume_locked(account_state, normalized_stock_code)
                if resolved_order_volume > available_volume:
                    rejection_payload = self._reject_existing_order_locked(
                        account_state=account_state,
                        order_state=order_state,
                        error_msg="可用持仓不足，纸面卖出委托被拒绝。",
                        error_id=DEFAULT_ORDER_ERROR_ID,
                    )
                elif position_state is not None:
                    # 先冻结可卖持仓，避免连续卖单把同一份持仓重复占用。
                    self._reserve_sell_volume_locked(position_state, resolved_order_volume)

            order_snapshot = self._build_order_snapshot_locked(order_state)

        if rejection_payload is not None:
            rejection_order_snapshot, rejection_error_snapshot = rejection_payload
            self._safe_emit("on_order_error", rejection_error_snapshot)
            self._safe_emit("on_stock_order", rejection_order_snapshot)
            return _as_int(order_id, -1)

        self._safe_emit("on_stock_order", order_snapshot)
        worker = threading.Thread(
            target=self._run_order_lifecycle,
            args=(account_key, order_id),
            daemon=True,
            name=f"paper-trader-order-{order_id}",
        )
        worker.start()
        return order_id

    def order_stock_async(
        self,
        account: Any,
        stock_code: str,
        order_type: int,
        order_volume: int,
        price_type: int,
        price: float,
        strategy_name: str = "",
        order_remark: str = "",
        callback: Any = None,
    ) -> int:
        """异步提交股票买卖委托。"""

        order_id = self.order_stock(
            account,
            stock_code,
            order_type,
            order_volume,
            price_type,
            price,
            strategy_name,
            order_remark,
        )
        account_state, _ = self._get_or_create_account_state(account)
        order_state = self._get_order_state_by_id(account_state, order_id)
        with self._lock:
            seq = self._next_async_seq_locked()
        response = PaperOrderResponse(
            account_type=account_state.account_type,
            account_id=account_state.account_id,
            order_id=order_id,
            strategy_name=_as_str(strategy_name),
            order_remark=_as_str(order_remark),
            error_msg=order_state.reject_error_msg if order_state and not order_state.accepted else "",
            seq=seq,
            order_sysid=order_state.order_sysid if order_state is not None else self._build_order_sysid(order_id),
        )
        return self._schedule_async_callback(callback, response, seq)

    def cancel_order_stock(self, account: Any, order_id: int) -> int:
        """提交撤单请求。"""

        account_state, _ = self._get_or_create_account_state(account)
        cancel_error_payload: PaperCancelError | None = None
        order_snapshot: PaperOrder | None = None
        with self._lock:
            order_state = account_state.orders.get(_as_int(order_id, -1))
            if order_state is None:
                cancel_error_payload = PaperCancelError(
                    account_type=account_state.account_type,
                    account_id=account_state.account_id,
                    order_id=_as_int(order_id, -1),
                    market=0,
                    order_sysid="",
                    error_id=DEFAULT_CANCEL_ERROR_ID,
                    error_msg="未找到可撤销的委托。",
                    order_status=0,
                )
            elif order_state.order_status in self._constants.terminal_order_statuses:
                cancel_error_payload = PaperCancelError(
                    account_type=account_state.account_type,
                    account_id=account_state.account_id,
                    order_id=order_state.order_id,
                    market=0,
                    order_sysid=order_state.order_sysid,
                    error_id=DEFAULT_CANCEL_ERROR_ID,
                    error_msg="委托已进入终态，无法撤单。",
                    order_status=order_state.order_status,
                )
            else:
                order_state.cancel_requested = True
                order_state.cancel_requested_at = time.time()
                if order_state.traded_volume > 0:
                    order_state.order_status = self._constants.order_partsucc_cancel
                    order_state.status_msg = "部成待撤"
                else:
                    order_state.order_status = self._constants.order_reported_cancel
                    order_state.status_msg = "已报待撤"
                order_snapshot = self._build_order_snapshot_locked(order_state)

        if cancel_error_payload is not None:
            self._safe_emit("on_cancel_error", cancel_error_payload)
            return -1
        if order_snapshot is not None:
            self._safe_emit("on_stock_order", order_snapshot)
            return 0
        return -1

    def cancel_order_stock_async(
        self,
        account: Any,
        order_id: int,
        callback: Any = None,
    ) -> int:
        """异步提交撤单请求。"""

        account_state, _ = self._get_or_create_account_state(account)
        cancel_result = self.cancel_order_stock(account, order_id)
        with self._lock:
            order_state = account_state.orders.get(_as_int(order_id, -1))
            order_sysid = order_state.order_sysid if order_state is not None else ""
            seq = self._next_async_seq_locked()
        response = PaperCancelOrderResponse(
            account_type=account_state.account_type,
            account_id=account_state.account_id,
            cancel_result=cancel_result,
            order_id=_as_int(order_id, -1),
            order_sysid=order_sysid,
            seq=seq,
            error_msg="" if cancel_result == 0 else "撤单失败或订单已进入终态。",
        )
        return self._schedule_async_callback(callback, response, seq)

    def cancel_order_stock_sysid(self, account: Any, market: Any, sysid: str) -> int:
        """按柜台编号撤单。"""

        account_state, _ = self._get_or_create_account_state(account)
        cancel_error_payload: PaperCancelError | None = None
        order_state: _OrderState | None = None
        with self._lock:
            order_ref = self._orders_by_sysid.get(_as_str(sysid))
            if order_ref is None:
                cancel_error_payload = PaperCancelError(
                    account_type=account_state.account_type,
                    account_id=account_state.account_id,
                    order_id=-1,
                    market=_as_int(market, 0),
                    order_sysid=_as_str(sysid),
                    error_id=DEFAULT_CANCEL_ERROR_ID,
                    error_msg="未找到可撤销的柜台编号。",
                    order_status=0,
                )
            else:
                order_key, order_id = order_ref
                if order_key != self._get_account_key(account):
                    cancel_error_payload = PaperCancelError(
                        account_type=account_state.account_type,
                        account_id=account_state.account_id,
                        order_id=order_id,
                        market=_as_int(market, 0),
                        order_sysid=_as_str(sysid),
                        error_id=DEFAULT_CANCEL_ERROR_ID,
                        error_msg="柜台编号不属于当前账户，无法撤单。",
                        order_status=0,
                    )
                else:
                    order_state = account_state.orders.get(order_id)
                    if order_state is None:
                        cancel_error_payload = PaperCancelError(
                            account_type=account_state.account_type,
                            account_id=account_state.account_id,
                            order_id=order_id,
                            market=_as_int(market, 0),
                            order_sysid=_as_str(sysid),
                            error_id=DEFAULT_CANCEL_ERROR_ID,
                            error_msg="委托已不存在，无法撤单。",
                            order_status=0,
                        )

        if cancel_error_payload is not None:
            self._safe_emit("on_cancel_error", cancel_error_payload)
            return -1

        if order_state is None:
            return -1

        return self.cancel_order_stock(account, order_state.order_id)

    def cancel_order_stock_sysid_async(
        self,
        account: Any,
        market: Any,
        sysid: str,
        callback: Any = None,
    ) -> int:
        """按柜台编号异步撤单。"""

        cancel_result = self.cancel_order_stock_sysid(account, market, sysid)
        account_state, _ = self._get_or_create_account_state(account)
        with self._lock:
            order_ref = self._orders_by_sysid.get(_as_str(sysid))
            order_id = order_ref[1] if order_ref is not None else -1
            seq = self._next_async_seq_locked()
        response = PaperCancelOrderResponse(
            account_type=account_state.account_type,
            account_id=account_state.account_id,
            cancel_result=cancel_result,
            order_id=order_id,
            order_sysid=_as_str(sysid),
            seq=seq,
            error_msg="" if cancel_result == 0 else "撤单失败或订单已进入终态。",
        )
        return self._schedule_async_callback(callback, response, seq)

    def query_credit_detail(self, account: Any) -> dict[str, Any]:
        """兼容接口：纸面交易下信用详情暂不展开。"""

        return {}

    def query_credit_detail_async(self, account: Any, callback: Any = None) -> int:
        """兼容接口：信用详情异步查询。"""

        return self._schedule_async_result(callback, self.query_credit_detail(account))

    def query_stk_compacts(self, account: Any) -> list[Any]:
        """兼容接口：纸面交易下不模拟合约列表。"""

        return []

    def query_stk_compacts_async(self, account: Any, callback: Any = None) -> int:
        """兼容接口：纸面交易下合约列表异步查询。"""

        return self._schedule_async_result(callback, self.query_stk_compacts(account))

    def query_credit_subjects(self, account: Any) -> list[Any]:
        """兼容接口：纸面交易下不模拟信用标的池。"""

        return []

    def query_credit_subjects_async(self, account: Any, callback: Any = None) -> int:
        """兼容接口：纸面交易下信用标的池异步查询。"""

        return self._schedule_async_result(callback, self.query_credit_subjects(account))

    def query_credit_slo_code(self, account: Any) -> list[Any]:
        """兼容接口：纸面交易下不模拟融券标的。"""

        return []

    def query_credit_slo_code_async(self, account: Any, callback: Any = None) -> int:
        """兼容接口：纸面交易下融券标的异步查询。"""

        return self._schedule_async_result(callback, self.query_credit_slo_code(account))

    def query_credit_assure(self, account: Any) -> list[Any]:
        """兼容接口：纸面交易下不模拟担保品。"""

        return []

    def query_credit_assure_async(self, account: Any, callback: Any = None) -> int:
        """兼容接口：纸面交易下担保品异步查询。"""

        return self._schedule_async_result(callback, self.query_credit_assure(account))

    def query_new_purchase_limit(self, account: Any) -> dict[str, Any]:
        """兼容接口：纸面交易下不模拟新股申购额度。"""

        return {}

    def query_new_purchase_limit_async(self, account: Any, callback: Any = None) -> int:
        """兼容接口：纸面交易下新股申购额度异步查询。"""

        return self._schedule_async_result(callback, self.query_new_purchase_limit(account))

    def _schedule_async_result(self, callback: Any, result: Any, seq: int | None = None) -> int:
        """异步回调一个查询结果。"""

        if seq is None:
            seq = self._next_async_seq()
        if callback is None:
            return seq
        thread = threading.Thread(
            target=self._invoke_callback_later,
            args=(callback, result, 0.0),
            daemon=True,
            name=f"paper-trader-async-{seq}",
        )
        thread.start()
        return seq

    def _schedule_async_callback(self, callback: Any, payload: Any, seq: int | None = None) -> int:
        """异步回调一个响应对象。"""

        if seq is None:
            seq = self._next_async_seq()
        if callback is None:
            return seq
        thread = threading.Thread(
            target=self._invoke_callback_later,
            args=(callback, payload, 0.0),
            daemon=True,
            name=f"paper-trader-callback-{seq}",
        )
        thread.start()
        return seq

    def _invoke_callback_later(self, callback: Any, payload: Any, delay_seconds: float) -> None:
        """在后台线程里触发回调，避免阻塞下单主流程。"""

        if delay_seconds > 0:
            time.sleep(delay_seconds)
        try:
            callback(payload)
        except Exception:
            LOGGER.exception("纸面交易异步回调失败: callback=%s", type(callback).__name__)

    def _safe_emit(self, method_name: str, *args: Any) -> None:
        """安全触发交易回调，避免背景线程因为回调异常提前退出。"""

        callback = None
        with self._lock:
            callback = self._callback
        if callback is None:
            return
        handler = getattr(callback, method_name, None)
        if not callable(handler):
            return
        try:
            handler(*args)
        except Exception:
            LOGGER.exception("纸面交易推送失败: event=%s", method_name)

    def _next_order_id_locked(self) -> int:
        """生成新的委托号。"""

        self._order_seq += 1
        return self._order_seq

    def _next_async_seq(self) -> int:
        """生成新的异步请求序号。"""

        with self._lock:
            return self._next_async_seq_locked()

    def _next_async_seq_locked(self) -> int:
        """在持锁状态下生成新的异步请求序号。"""

        self._async_seq += 1
        return self._async_seq

    def _build_order_sysid(self, order_id: int) -> str:
        """构造模拟柜台编号。"""

        session_part = _as_int(self.session_id, 0)
        return f"PS{session_part:06d}-{order_id:08d}"

    def _get_account_key(self, account: Any | None) -> tuple[str, str]:
        """生成账户状态字典的键。"""

        account_id = _as_str(self._read_field(account, "account_id", "DEFAULT"))
        account_type = _resolve_account_type(self._read_field(account, "account_type", "STOCK"))
        return account_id, _as_str(account_type)

    def _read_field(self, value: Any, field_name: str, default: Any = None) -> Any:
        """兼容字典和对象的字段读取。"""

        if value is None:
            return default
        if isinstance(value, Mapping):
            return value.get(field_name, default)
        return getattr(value, field_name, default)

    def _clone_seed_positions(self) -> dict[str, _PositionState]:
        """把初始化种子转换成可变的持仓状态。"""

        positions: dict[str, _PositionState] = {}
        for seed_position in self._seed.positions:
            stock_code = _normalize_stock_code(seed_position.stock_code)
            volume = max(_as_int(seed_position.volume), 0)
            if stock_code == "" or volume <= 0:
                continue
            can_use_volume = volume if seed_position.can_use_volume is None else max(_as_int(seed_position.can_use_volume), 0)
            can_use_volume = min(can_use_volume, volume)
            avg_price = max(_as_float(seed_position.avg_price), 0.0)
            last_price = seed_position.last_price if seed_position.last_price is not None else avg_price
            positions[stock_code] = _PositionState(
                stock_code=stock_code,
                volume=volume,
                can_use_volume=can_use_volume,
                avg_price=avg_price,
                last_price=max(_as_float(last_price), 0.0),
                yesterday_volume=(
                    max(_as_int(seed_position.yesterday_volume), 0)
                    if seed_position.yesterday_volume is not None
                    else volume
                ),
            )
        return positions

    def _create_account_state(self, account: Any | None) -> _AccountState:
        """创建新账户的内部状态。"""

        account_id = _as_str(self._read_field(account, "account_id", "DEFAULT"))
        account_type = _resolve_account_type(self._read_field(account, "account_type", "STOCK"))
        return _AccountState(
            account_id=account_id,
            account_type=account_type,
            cash=self._seed.cash,
            frozen_cash=self._seed.frozen_cash,
            positions=self._clone_seed_positions(),
            status=self._constants.account_status_ok,
        )

    def _get_or_create_account_state(self, account: Any | None) -> tuple[_AccountState, tuple[str, str]]:
        """按账户对象取出内部状态。"""

        account_key = self._get_account_key(account)
        with self._lock:
            state = self._accounts.get(account_key)
            if state is None:
                state = self._create_account_state(account)
                self._accounts[account_key] = state
            return state, account_key

    def _build_account_status_snapshot(self, state: _AccountState) -> PaperAccountStatus:
        """构造账户状态快照。"""

        return PaperAccountStatus(
            account_type=state.account_type,
            account_id=state.account_id,
            status=state.status,
        )

    def _build_account_info_snapshot(self, state: _AccountState) -> PaperAccountInfo:
        """构造账户信息快照。"""

        asset = self._build_asset_snapshot_locked(state)
        return PaperAccountInfo(
            account_type=state.account_type,
            account_id=state.account_id,
            status=state.status,
            cash=asset.cash,
            frozen_cash=asset.frozen_cash,
            market_value=asset.market_value,
            total_asset=asset.total_asset,
            fetch_balance=asset.fetch_balance,
        )

    def _build_asset_snapshot_locked(self, state: _AccountState) -> PaperAsset:
        """构造资金快照。"""

        market_value = 0.0
        for position_state in state.positions.values():
            market_value += position_state.volume * self._resolve_last_price(position_state)
        total_asset = state.cash + state.frozen_cash + market_value
        return PaperAsset(
            account_type=state.account_type,
            account_id=state.account_id,
            cash=round(state.cash, 2),
            frozen_cash=round(state.frozen_cash, 2),
            market_value=round(market_value, 2),
            total_asset=round(total_asset, 2),
            # 这里把 cash 视为可用资金，因此可取资金与可用资金保持一致。
            fetch_balance=round(state.cash, 2),
        )

    def _build_position_snapshot_locked(self, state: _AccountState, position_state: _PositionState) -> PaperPosition:
        """构造单个持仓快照。"""

        last_price = self._resolve_last_price(position_state)
        market_value = round(position_state.volume * last_price, 2)
        profit_rate = 0.0
        if position_state.avg_price > 0:
            profit_rate = round((last_price - position_state.avg_price) / position_state.avg_price, 6)
        return PaperPosition(
            account_type=state.account_type,
            account_id=state.account_id,
            stock_code=position_state.stock_code,
            volume=position_state.volume,
            can_use_volume=position_state.can_use_volume,
            open_price=round(position_state.avg_price, 4),
            market_value=market_value,
            frozen_volume=position_state.frozen_volume,
            on_road_volume=position_state.on_road_volume,
            yesterday_volume=position_state.yesterday_volume,
            avg_price=round(position_state.avg_price, 4),
            direction=0,
            last_price=round(last_price, 4),
            profit_rate=profit_rate,
            secu_account=state.account_id,
            instrument_name=self._resolve_instrument_name(position_state.stock_code),
        )

    def _build_position_snapshots_locked(self, state: _AccountState) -> list[PaperPosition]:
        """构造账户全部持仓快照。"""

        positions = sorted(state.positions.values(), key=lambda item: item.stock_code)
        return [self._build_position_snapshot_locked(state, position_state) for position_state in positions]

    def _build_order_snapshot_locked(self, order_state: _OrderState) -> PaperOrder:
        """构造委托快照。"""

        return PaperOrder(
            account_type=order_state.account_type,
            account_id=order_state.account_id,
            stock_code=order_state.stock_code,
            order_id=order_state.order_id,
            order_sysid=order_state.order_sysid,
            order_time=order_state.order_time,
            order_type=order_state.order_type,
            order_volume=order_state.order_volume,
            price_type=order_state.price_type,
            price=round(order_state.price, 4),
            traded_volume=order_state.traded_volume,
            traded_price=round(order_state.traded_price, 4),
            order_status=order_state.order_status,
            status_msg=order_state.status_msg,
            strategy_name=order_state.strategy_name,
            order_remark=order_state.order_remark,
            direction=order_state.direction,
            offset_flag=order_state.offset_flag,
            secu_account=order_state.secu_account,
            instrument_name=order_state.instrument_name,
        )

    def _build_trade_snapshot_locked(self, trade_state: _TradeState) -> PaperTrade:
        """构造成交快照。"""

        return PaperTrade(
            account_type=trade_state.account_type,
            account_id=trade_state.account_id,
            stock_code=trade_state.stock_code,
            order_type=trade_state.order_type,
            traded_id=trade_state.traded_id,
            traded_time=trade_state.traded_time,
            traded_price=round(trade_state.traded_price, 4),
            traded_volume=trade_state.traded_volume,
            traded_amount=round(trade_state.traded_amount, 2),
            order_id=trade_state.order_id,
            order_sysid=trade_state.order_sysid,
            strategy_name=trade_state.strategy_name,
            order_remark=trade_state.order_remark,
            direction=trade_state.direction,
            offset_flag=trade_state.offset_flag,
            commission=round(trade_state.commission, 2),
            secu_account=trade_state.secu_account,
            instrument_name=trade_state.instrument_name,
        )

    def _resolve_instrument_name(self, stock_code: str) -> str:
        """尽量从 xtdata 中解析证券名称。"""

        normalized_stock_code = _normalize_stock_code(stock_code)
        if not normalized_stock_code:
            return ""
        with self._lock:
            cached_name = self._instrument_name_cache.get(normalized_stock_code)
        if cached_name is not None:
            return cached_name

        resolved_name = normalized_stock_code
        xtdata = self._xtdata
        if xtdata is not None and callable(getattr(xtdata, "get_instrument_detail", None)):
            try:
                info = xtdata.get_instrument_detail(normalized_stock_code)
                if isinstance(info, Mapping):
                    resolved_name = _as_str(info.get("InstrumentName"), normalized_stock_code) or normalized_stock_code
                else:
                    resolved_name = _as_str(getattr(info, "InstrumentName", normalized_stock_code), normalized_stock_code)
            except Exception:
                resolved_name = normalized_stock_code

        with self._lock:
            self._instrument_name_cache[normalized_stock_code] = resolved_name
        return resolved_name

    def _resolve_last_price(self, position_state: _PositionState) -> float:
        """尽量用真实 xtdata 最新价更新持仓市值。"""

        last_price = position_state.last_price
        xtdata = self._xtdata
        if xtdata is not None and callable(getattr(xtdata, "get_full_tick", None)):
            try:
                tick_data = xtdata.get_full_tick([position_state.stock_code])
                if isinstance(tick_data, Mapping):
                    item = tick_data.get(position_state.stock_code)
                else:
                    item = getattr(tick_data, position_state.stock_code, None)
                if isinstance(item, Mapping):
                    candidate_price = item.get("lastPrice")
                else:
                    candidate_price = getattr(item, "lastPrice", None)
                candidate_price = _as_float(candidate_price, 0.0)
                if candidate_price > 0:
                    last_price = candidate_price
            except Exception:
                pass
        if last_price <= 0:
            last_price = position_state.avg_price
        return max(last_price, 0.0)

    def _resolve_execution_price(self, stock_code: str, price_type: int, order_price: float) -> float:
        """解析纸面交易的执行价格。"""

        if price_type == self._constants.fix_price:
            return max(_as_float(order_price, 0.0), 0.0)

        latest_price = 0.0
        xtdata = self._xtdata
        if xtdata is not None and callable(getattr(xtdata, "get_full_tick", None)):
            try:
                tick_data = xtdata.get_full_tick([stock_code])
                if isinstance(tick_data, Mapping):
                    item = tick_data.get(stock_code)
                else:
                    item = getattr(tick_data, stock_code, None)
                if isinstance(item, Mapping):
                    latest_price = _as_float(item.get("lastPrice"), 0.0)
                else:
                    latest_price = _as_float(getattr(item, "lastPrice", 0.0), 0.0)
            except Exception:
                latest_price = 0.0

        if latest_price > 0:
            return latest_price
        return max(_as_float(order_price, 0.0), 0.0)

    def _get_sellable_volume_locked(self, state: _AccountState, stock_code: str) -> int:
        """计算当前可卖数量。"""

        position_state = state.positions.get(stock_code)
        if position_state is None:
            return 0
        return min(position_state.volume, position_state.can_use_volume)

    def _check_buy_capacity_locked(self, state: _AccountState, order_volume: int, execution_price: float) -> bool:
        """判断当前现金是否足够买入。"""

        return state.cash >= round(order_volume * execution_price, 2)

    def _reserve_buy_cash_locked(
        self,
        state: _AccountState,
        order_volume: int,
        execution_price: float,
    ) -> float:
        """冻结买单所需资金。"""

        reserved_cash = round(order_volume * execution_price, 2)
        state.cash = round(state.cash - reserved_cash, 2)
        state.frozen_cash = round(state.frozen_cash + reserved_cash, 2)
        return reserved_cash

    def _release_buy_cash_locked(self, state: _AccountState, reserved_cash: float) -> None:
        """把未成交的买单冻结资金释放回可用资金。"""

        refund_cash = round(max(_as_float(reserved_cash, 0.0), 0.0), 2)
        if refund_cash <= 0:
            return
        state.frozen_cash = round(max(state.frozen_cash - refund_cash, 0.0), 2)
        state.cash = round(state.cash + refund_cash, 2)

    def _reserve_sell_volume_locked(self, position_state: _PositionState, order_volume: int) -> None:
        """冻结卖单所需持仓数量。"""

        position_state.can_use_volume = max(position_state.can_use_volume - order_volume, 0)
        position_state.frozen_volume = max(position_state.frozen_volume + order_volume, 0)

    def _release_sell_volume_locked(self, position_state: _PositionState, reserved_volume: int) -> None:
        """把未成交的卖单冻结持仓释放回可用数量。"""

        refund_volume = max(_as_int(reserved_volume, 0), 0)
        if refund_volume <= 0:
            return
        position_state.frozen_volume = max(position_state.frozen_volume - refund_volume, 0)
        position_state.can_use_volume = min(position_state.can_use_volume + refund_volume, position_state.volume)

    def _reject_existing_order_locked(
        self,
        account_state: _AccountState,
        order_state: _OrderState,
        error_msg: str,
        error_id: int,
    ) -> tuple[PaperOrder, PaperOrderError]:
        """把已经登记的委托标记成废单并返回快照。"""

        order_state.order_status = self._constants.order_junk
        order_state.status_msg = "废单"
        order_state.accepted = False
        order_state.reject_error_id = error_id
        order_state.reject_error_msg = error_msg
        order_snapshot = self._build_order_snapshot_locked(order_state)
        error_snapshot = PaperOrderError(
            account_type=account_state.account_type,
            account_id=account_state.account_id,
            order_id=order_state.order_id,
            error_id=error_id,
            error_msg=error_msg,
            strategy_name=order_state.strategy_name,
            order_remark=order_state.order_remark,
            order_status=order_state.order_status,
        )
        return order_snapshot, error_snapshot

    def _reject_order(
        self,
        account_state: _AccountState,
        account_key: tuple[str, str],
        stock_code: str,
        order_type: int,
        order_volume: int,
        price_type: int,
        price: float,
        strategy_name: str,
        order_remark: str,
        error_msg: str,
    ) -> int:
        """创建一个废单并直接返回。"""

        with self._lock:
            order_id = self._next_order_id_locked()
            order_sysid = self._build_order_sysid(order_id)
            order_state = _OrderState(
                account_key=account_key,
                account_id=account_state.account_id,
                account_type=account_state.account_type,
                stock_code=stock_code,
                order_id=order_id,
                order_sysid=order_sysid,
                order_time=_now_text(),
                order_type=order_type,
                order_volume=order_volume,
                price_type=price_type,
                price=price,
                strategy_name=_as_str(strategy_name),
                order_remark=_as_str(order_remark),
                instrument_name=self._resolve_instrument_name(stock_code),
                order_status=self._constants.order_junk,
                status_msg="废单",
                accepted=False,
                reject_error_id=DEFAULT_ORDER_ERROR_ID,
                reject_error_msg=error_msg,
                secu_account=account_state.account_id,
            )
            account_state.orders[order_id] = order_state
            self._orders_by_sysid[order_sysid] = (account_key, order_id)
            order_snapshot = self._build_order_snapshot_locked(order_state)
            error_snapshot = PaperOrderError(
                account_type=account_state.account_type,
                account_id=account_state.account_id,
                order_id=order_id,
                error_id=DEFAULT_ORDER_ERROR_ID,
                error_msg=error_msg,
                strategy_name=order_state.strategy_name,
                order_remark=order_state.order_remark,
                order_status=order_state.order_status,
            )

        self._safe_emit("on_order_error", error_snapshot)
        self._safe_emit("on_stock_order", order_snapshot)
        return order_id

    def _get_order_state_by_id(self, account_state: _AccountState, order_id: int) -> _OrderState | None:
        """按订单号从账户状态中取委托。"""

        with self._lock:
            return account_state.orders.get(order_id)

    def _run_order_lifecycle(self, account_key: tuple[str, str], order_id: int) -> None:
        """按“部分成交 -> 可撤 -> 终态”的顺序推进模拟柜台。"""

        time.sleep(max(self._fill_interval_seconds, 0.0))
        while not self._stop_event.is_set():
            with self._lock:
                account_state = self._accounts.get(account_key)
                if account_state is None:
                    return
                order_state = account_state.orders.get(order_id)
                if order_state is None:
                    return
                if not order_state.accepted:
                    return
                if order_state.order_status in self._constants.terminal_order_statuses:
                    return
                if order_state.cancel_requested:
                    break

                remaining_volume = max(order_state.order_volume - order_state.traded_volume, 0)
                if remaining_volume <= 0:
                    order_state.order_status = self._constants.order_succeeded
                    order_state.status_msg = "已成"
                    order_snapshot = self._build_order_snapshot_locked(order_state)
                    break

                fill_volume = self._build_next_fill_volume(remaining_volume)
                if fill_volume <= 0:
                    order_state.order_status = self._constants.order_junk
                    order_state.status_msg = "废单"
                    order_snapshot = self._build_order_snapshot_locked(order_state)
                    break

                trade_state = self._apply_fill_locked(account_state, order_state, fill_volume)
                order_snapshot = self._build_order_snapshot_locked(order_state)
                trade_snapshot = self._build_trade_snapshot_locked(trade_state)
                asset_snapshot = self._build_asset_snapshot_locked(account_state)
                position_snapshot = self._build_position_snapshot_after_fill_locked(account_state, order_state.stock_code)

            self._safe_emit("on_stock_trade", trade_snapshot)
            self._safe_emit("on_stock_order", order_snapshot)
            self._safe_emit("on_stock_asset", asset_snapshot)
            if position_snapshot is not None:
                self._safe_emit("on_stock_position", position_snapshot)

            if order_snapshot.order_status in self._constants.terminal_order_statuses:
                return

            time.sleep(max(self._fill_interval_seconds, 0.0))

        with self._lock:
            account_state = self._accounts.get(account_key)
            if account_state is None:
                return
            order_state = account_state.orders.get(order_id)
            if order_state is None or not order_state.accepted:
                return
            if order_state.order_status in self._constants.terminal_order_statuses:
                return
            if order_state.traded_volume > 0:
                order_state.order_status = self._constants.order_part_cancel
                order_state.status_msg = "部撤"
            else:
                order_state.order_status = self._constants.order_canceled
                order_state.status_msg = "已撤"
            if order_state.order_type == self._constants.stock_buy:
                remaining_volume = max(order_state.order_volume - order_state.traded_volume, 0)
                remaining_cash = round(remaining_volume * order_state.price, 2)
                self._release_buy_cash_locked(account_state, remaining_cash)
            else:
                position_state = account_state.positions.get(order_state.stock_code)
                if position_state is not None:
                    remaining_volume = max(order_state.order_volume - order_state.traded_volume, 0)
                    self._release_sell_volume_locked(position_state, remaining_volume)
                    if position_state.volume <= 0 and position_state.frozen_volume <= 0:
                        account_state.positions.pop(order_state.stock_code, None)
            order_snapshot = self._build_order_snapshot_locked(order_state)

        self._safe_emit("on_stock_order", order_snapshot)

    def _build_next_fill_volume(self, remaining_volume: int) -> int:
        """把剩余数量拆成一个可观测的部分成交切片。"""

        if remaining_volume <= 1:
            return remaining_volume
        if remaining_volume == 2:
            return 1

        first_cut = max(1, remaining_volume // 3)
        if first_cut >= remaining_volume:
            first_cut = remaining_volume - 1
        return max(first_cut, 1)

    def _apply_fill_locked(
        self,
        account_state: _AccountState,
        order_state: _OrderState,
        fill_volume: int,
    ) -> _TradeState:
        """在持锁状态下应用一次成交，并更新资金与持仓。"""

        fill_price = order_state.price
        fill_amount = round(fill_volume * fill_price, 2)
        order_state.traded_volume += fill_volume
        order_state.traded_amount = round(order_state.traded_amount + fill_amount, 2)
        order_state.trade_count += 1
        order_state.traded_price = round(
            order_state.traded_amount / order_state.traded_volume if order_state.traded_volume > 0 else 0.0,
            4,
        )

        position_state = account_state.positions.get(order_state.stock_code)
        if order_state.order_type == self._constants.stock_buy:
            account_state.frozen_cash = round(max(account_state.frozen_cash - fill_amount, 0.0), 2)
            if position_state is None:
                position_state = _PositionState(
                    stock_code=order_state.stock_code,
                    volume=0,
                    can_use_volume=0,
                    avg_price=0.0,
                    last_price=fill_price,
                    yesterday_volume=0,
                )
                account_state.positions[order_state.stock_code] = position_state

            total_volume = position_state.volume + fill_volume
            if total_volume <= 0:
                total_volume = fill_volume
            position_state.avg_price = round(
                (
                    position_state.avg_price * position_state.volume + fill_price * fill_volume
                ) / total_volume,
                4,
            )
            position_state.volume = total_volume
            position_state.can_use_volume = position_state.volume - position_state.frozen_volume
            position_state.last_price = fill_price
            if position_state.yesterday_volume <= 0:
                position_state.yesterday_volume = 0
            order_state.status_msg = "部成" if order_state.traded_volume < order_state.order_volume else "已成"
        else:
            if position_state is None:
                position_state = _PositionState(
                    stock_code=order_state.stock_code,
                    volume=0,
                    can_use_volume=0,
                    avg_price=0.0,
                    last_price=fill_price,
                    yesterday_volume=0,
                )
                account_state.positions[order_state.stock_code] = position_state

            position_state.frozen_volume = max(position_state.frozen_volume - fill_volume, 0)
            position_state.volume = max(position_state.volume - fill_volume, 0)
            position_state.last_price = fill_price
            if position_state.volume <= 0 and position_state.frozen_volume <= 0:
                account_state.positions.pop(order_state.stock_code, None)
            order_state.status_msg = "部成" if order_state.traded_volume < order_state.order_volume else "已成"

        if order_state.traded_volume < order_state.order_volume:
            order_state.order_status = self._constants.order_part_succ
        else:
            order_state.order_status = self._constants.order_succeeded
        trade_state = self._create_trade_state_locked(account_state, order_state, fill_volume, fill_price, fill_amount)
        account_state.trades.append(trade_state)
        return trade_state

    def _create_trade_state_locked(
        self,
        account_state: _AccountState,
        order_state: _OrderState,
        fill_volume: int,
        fill_price: float,
        fill_amount: float,
    ) -> _TradeState:
        """创建一次成交内部状态。"""

        trade_index = order_state.trade_count
        traded_id = f"T{order_state.order_id:08d}-{trade_index:02d}"
        return _TradeState(
            account_key=order_state.account_key,
            account_id=account_state.account_id,
            account_type=account_state.account_type,
            stock_code=order_state.stock_code,
            order_type=order_state.order_type,
            traded_id=traded_id,
            traded_time=_now_text(),
            traded_price=fill_price,
            traded_volume=fill_volume,
            traded_amount=fill_amount,
            order_id=order_state.order_id,
            order_sysid=order_state.order_sysid,
            strategy_name=order_state.strategy_name,
            order_remark=order_state.order_remark,
            direction=order_state.direction,
            offset_flag=order_state.offset_flag,
            commission=0.0,
            secu_account=order_state.secu_account,
            instrument_name=order_state.instrument_name,
        )

    def _build_position_snapshot_after_fill_locked(
        self,
        account_state: _AccountState,
        stock_code: str,
    ) -> PaperPosition | None:
        """在成交后构造受影响持仓的快照。"""

        position_state = account_state.positions.get(stock_code)
        if position_state is None:
            return PaperPosition(
                account_type=account_state.account_type,
                account_id=account_state.account_id,
                stock_code=stock_code,
                volume=0,
                can_use_volume=0,
                open_price=0.0,
                market_value=0.0,
                frozen_volume=0,
                on_road_volume=0,
                yesterday_volume=0,
                avg_price=0.0,
                direction=0,
                last_price=0.0,
                profit_rate=0.0,
                secu_account=account_state.account_id,
                instrument_name=self._resolve_instrument_name(stock_code),
            )
        return self._build_position_snapshot_locked(account_state, position_state)

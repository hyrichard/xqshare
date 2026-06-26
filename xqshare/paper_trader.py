"""纸面交易模拟核心。

本模块只负责模拟交易的状态管理和撮合逻辑，不处理回调派发、
线程调度或网络桥接。这些职责由 server.py 的 TraderBridge 承担，
确保模拟路径与真实柜台路径走同一套回调链路。

主要职责：
1. 管理账户资金、持仓、委托和成交的内部状态。
2. 模拟下单验证、资金冻结、部分成交和撤单。
3. 提供 tick() 方法推进订单生命周期，返回待推送的事件列表。
4. 提供查询接口返回状态快照。
"""

from __future__ import annotations

import json
import logging
import os
import threading
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Mapping, Optional
from concurrent.futures import Future, ThreadPoolExecutor

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


# ==================== 常量定义 ====================


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
        return frozenset({self.order_part_cancel, self.order_canceled, self.order_succeeded, self.order_junk})

    @property
    def cancelable_order_statuses(self) -> frozenset[int]:
        """返回仍可继续撤单的状态集合。"""
        return frozenset({
            self.order_reported, self.order_reported_cancel,
            self.order_partsucc_cancel, self.order_part_succ,
        })

    @property
    def market_order_types(self) -> frozenset[int]:
        """返回当前纸面交易识别为"按行情成交"的报价类型集合。"""
        return frozenset({
            self.latest_price, self.market_peer_price_first,
            self.market_sz_instbusi_restcancel, self.market_sz_full_or_cancel,
        })


# ==================== 种子与快照数据结构 ====================


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
                    positions.append(PaperSeedPosition(
                        stock_code=_normalize_stock_code(stock_code),
                        volume=_as_int(value.get("volume", 0)),
                        avg_price=_as_float(value.get("avg_price", 0.0)),
                        can_use_volume=(
                            _as_int(value.get("can_use_volume", 0))
                            if value.get("can_use_volume") is not None else None
                        ),
                        last_price=(
                            _as_float(value.get("last_price", 0.0))
                            if value.get("last_price") is not None else None
                        ),
                        yesterday_volume=(
                            _as_int(value.get("yesterday_volume", 0))
                            if value.get("yesterday_volume") is not None else None
                        ),
                    ))
                else:
                    positions.append(PaperSeedPosition(
                        stock_code=_normalize_stock_code(stock_code),
                        volume=_as_int(value),
                    ))
        elif isinstance(raw_positions, (list, tuple)):
            for item in raw_positions:
                if isinstance(item, Mapping):
                    stock_code = _normalize_stock_code(item.get("stock_code"))
                    if not stock_code:
                        continue
                    positions.append(PaperSeedPosition(
                        stock_code=stock_code,
                        volume=_as_int(item.get("volume", 0)),
                        avg_price=_as_float(item.get("avg_price", 0.0)),
                        can_use_volume=(
                            _as_int(item.get("can_use_volume", 0))
                            if item.get("can_use_volume") is not None else None
                        ),
                        last_price=(
                            _as_float(item.get("last_price", 0.0))
                            if item.get("last_price") is not None else None
                        ),
                        yesterday_volume=(
                            _as_int(item.get("yesterday_volume", 0))
                            if item.get("yesterday_volume") is not None else None
                        ),
                    ))
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


# ==================== 快照类型（对外返回） ====================


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


# ==================== 内部状态 ====================


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


# ==================== 事件类型 ====================


@dataclass
class PaperEvent:
    """纸面交易推送事件，由 TraderBridge 通过 CallbackManager 派发。"""
    event_name: str
    data: Any


# ==================== 核心模拟器 ====================


class PaperSimulator:
    """纸面交易模拟器。

    只负责账户状态管理和撮合逻辑，不处理回调派发和线程调度。
    所有模拟操作的结果通过返回值交给调用方（server.py 的 TraderBridge），
    由 TraderBridge 决定如何派发回调和推送事件。

    外部通过调用 tick() 推进活跃订单的生命周期，
    tick() 返回待推送的事件列表，由调用方负责派发。
    """

    def __init__(
        self,
        xtdata_module: Any = None,
        xtconstant_module: Any = None,
        seed: PaperTraderSeed | Mapping[str, Any] | None = None,
        fill_interval_seconds: float | None = None,
    ) -> None:
        """初始化模拟器。"""
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
        self._order_seq = 0
        self._accounts: dict[tuple[str, str], _AccountState] = {}
        self._orders_by_sysid: dict[str, tuple[tuple[str, str], int]] = {}
        self._instrument_name_cache: dict[str, str] = {}
        # 对齐 xtquant 的 ThreadPoolExecutor(max_workers=1) 机制：
        # 推送回调和 cancel 响应共享同一个单线程池，当推送占住线程时 cancel 阻塞。
        self._executor: ThreadPoolExecutor | None = None
        self._cancel_cbs: dict[int, Any] = {}  # seq -> callback

    @classmethod
    def from_env(
        cls,
        xtdata_module: Any = None,
        xtconstant_module: Any = None,
    ) -> "PaperSimulator":
        """从环境变量构造模拟器。"""
        seed = PaperTraderSeed.from_env(os.environ.get(PAPER_TRADER_SEED_ENV))
        fill_interval = _as_float(
            os.environ.get(PAPER_TRADER_FILL_INTERVAL_ENV),
            DEFAULT_FILL_INTERVAL_SECONDS,
        )
        return cls(
            xtdata_module=xtdata_module,
            xtconstant_module=xtconstant_module,
            seed=seed,
            fill_interval_seconds=fill_interval,
        )

    @property
    def fill_interval_seconds(self) -> float:
        """撮合间隔，供外部 ticker 线程使用。"""
        return self._fill_interval_seconds

    @property
    def constants(self) -> PaperTraderConstants:
        """常量集合，供外部构造响应对象。"""
        return self._constants

    @property
    def seed(self) -> PaperTraderSeed:
        """初始种子数据。"""
        return self._seed

    def stop(self) -> None:
        """标记模拟器停止，关闭 executor 和 tick 循环。"""
        self._stop_event.set()
        if self._executor is not None:
            self._executor.shutdown(wait=False)
            self._executor = None

    # ==================== 账户管理 ====================

    def subscribe_account(self, account: Any) -> list[PaperEvent]:
        """订阅账户，初始化内部状态并返回 on_account_status 事件。"""
        state, _ = self._get_or_create_account_state(account)
        state.subscribed = True
        state.status = self._constants.account_status_ok
        return [PaperEvent("on_account_status", self._build_account_status_snapshot(state))]

    def unsubscribe_account(self, account: Any) -> None:
        """取消订阅账户。"""
        _, account_key = self._get_or_create_account_state(account)
        with self._lock:
            state = self._accounts.get(account_key)
            if state is not None:
                state.subscribed = False

    # ==================== 交易操作 ====================

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
    ) -> tuple[int, list[PaperEvent]]:
        """提交股票买卖委托，返回 (order_id, events)。

        调用方负责派发 events 中的回调事件。
        """
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

        events: list[PaperEvent] = []
        order_id: int | None = None
        with self._lock:
            order_id = self._next_order_id_locked()
            order_sysid = self._build_order_sysid(order_id)
            instrument_name = self._resolve_instrument_name(normalized_stock_code)
            account_type = account_state.account_type

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

            rejection_payload: tuple[PaperOrder, PaperOrderError] | None = None
            if resolved_order_type == self._constants.stock_buy:
                can_accept = self._check_buy_capacity_locked(account_state, resolved_order_volume, execution_price)
                if can_accept:
                    # 先冻结资金，再让后台线程分步撮合，避免并发委托重复占用同一笔现金
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
                    # 先冻结可卖持仓，避免连续卖单重复占用同一份持仓
                    self._reserve_sell_volume_locked(position_state, resolved_order_volume)

            order_snapshot = self._build_order_snapshot_locked(order_state)

        if rejection_payload is not None:
            rejection_order_snapshot, rejection_error_snapshot = rejection_payload
            events.append(PaperEvent("on_order_error", rejection_error_snapshot))
            events.append(PaperEvent("on_stock_order", rejection_order_snapshot))
            return _as_int(order_id, -1), events

        events.append(PaperEvent("on_stock_order", order_snapshot))
        return order_id, events

    def cancel_order_stock(self, account: Any, order_id: int) -> tuple[int, list[PaperEvent]]:
        """提交撤单请求，返回 (cancel_result, events)。

        模拟真实柜台行为：始终返回 0 表示撤单请求已提交。
        撤单被拒（订单不存在/已终态/已全部成交）通过 on_cancel_error 回调通知，
        不由返回值表达。拒绝事件由 cancel_order_stock_sync_check 产生，
        由调用方 _paper_cancel_order_stock 在 RPC 返回前同步推送。
        """
        account_state, _ = self._get_or_create_account_state(account)
        with self._lock:
            order_state = account_state.orders.get(_as_int(order_id, -1))
            if order_state is None:
                return 0, []

            if order_state.order_status in self._constants.terminal_order_statuses:
                return 0, []

            # 标记撤单请求已提交，状态和事件由 ticker 异步推进
            order_state.cancel_requested = True

        # 返回 0 表示撤单请求已提交，与真实柜台 cancel_result=0 语义一致
        return 0, []

    def cancel_order_stock_sync_check(self, account: Any, order_id: int) -> list[PaperEvent]:
        """撤单同步校验，返回柜台当场拒绝时产生的 on_cancel_error 事件。

        模拟真实柜台在 cancel_order_stock 调用内部同步推送拒绝回调的行为。
        只在以下情况返回事件：
        - 订单不存在 → on_cancel_error（找不到可撤销的委托）
        - 订单已进入终态 → on_cancel_error（委托已进入终态，无法撤单）
        订单可撤时返回空列表，拒绝事件由 ticker 异步产生。
        """
        account_state, _ = self._get_or_create_account_state(account)
        with self._lock:
            order_state = account_state.orders.get(_as_int(order_id, -1))
            if order_state is None:
                return [PaperEvent("on_cancel_error", PaperCancelError(
                    account_type=account_state.account_type,
                    account_id=account_state.account_id,
                    order_id=_as_int(order_id, -1),
                    market=0,
                    order_sysid="",
                    error_id=DEFAULT_CANCEL_ERROR_ID,
                    error_msg="未找到可撤销的委托。",
                    order_status=0,
                ))]

            if order_state.order_status in self._constants.terminal_order_statuses:
                return [PaperEvent("on_cancel_error", PaperCancelError(
                    account_type=account_state.account_type,
                    account_id=account_state.account_id,
                    order_id=order_state.order_id,
                    market=0,
                    order_sysid=order_state.order_sysid,
                    error_id=DEFAULT_CANCEL_ERROR_ID,
                    error_msg="委托已进入终态，无法撤单。",
                    order_status=order_state.order_status,
                ))]

        return []

    def cancel_order_stock_sysid(self, account: Any, market: Any, sysid: str) -> tuple[int, list[PaperEvent]]:
        """按系统委托编号撤单。始终返回 0，拒绝事件由 sync_check 产生。"""
        account_state, _ = self._get_or_create_account_state(account)
        with self._lock:
            entry = self._orders_by_sysid.get(_as_str(sysid))
        if entry is None:
            return 0, []
        _, order_id = entry
        return self.cancel_order_stock(account, order_id)

    def cancel_order_stock_sysid_sync_check(self, account: Any, market: Any, sysid: str) -> list[PaperEvent]:
        """按系统编号撤单的同步校验，返回拒绝事件。"""
        account_state, _ = self._get_or_create_account_state(account)
        with self._lock:
            entry = self._orders_by_sysid.get(_as_str(sysid))
        if entry is None:
            return [PaperEvent("on_cancel_error", PaperCancelError(
                account_type=account_state.account_type,
                account_id=account_state.account_id,
                order_id=0,
                market=_as_int(market, 0),
                order_sysid=_as_str(sysid),
                error_id=DEFAULT_CANCEL_ERROR_ID,
                error_msg="未找到可撤销的委托。",
                order_status=0,
            ))]
        _, order_id = entry
        return self.cancel_order_stock_sync_check(account, order_id)

    # ==================== 带同步机制的撤单（对齐 xtquant executor + Future） ====================

    def _ensure_executor(self) -> ThreadPoolExecutor:
        """延迟创建单线程池，对齐 xtquant 的 ThreadPoolExecutor(max_workers=1)。"""
        if self._executor is None:
            self._executor = ThreadPoolExecutor(max_workers=1)
        return self._executor

    def cancel_order_stock_with_sync(
        self, account: Any, order_id: int, emit_events: Any,
    ) -> int:
        """带同步机制的撤单，对齐 xtquant 的 common_op_sync_with_seq。

        Args:
            account: 证券账户。
            order_id: 委托编号。
            emit_events: 事件派发回调，由 server.py 提供。

        Returns:
            int: cancel_result。
        """
        import random
        seq = random.randint(100000, 999999)
        future: Future = Future()
        self._cancel_cbs[seq] = lambda cancel_result: future.set_result(cancel_result)
        self._ensure_executor().submit(
            self._do_cancel_order_stock_sync, account, order_id, emit_events, seq,
        )
        return future.result()

    def cancel_order_stock_sysid_with_sync(
        self, account: Any, market: Any, sysid: str, emit_events: Any,
    ) -> int:
        """按系统编号撤单，对齐 xtquant 的 common_op_sync_with_seq。"""
        import random
        seq = random.randint(100000, 999999)
        future: Future = Future()
        self._cancel_cbs[seq] = lambda cancel_result: future.set_result(cancel_result)
        self._ensure_executor().submit(
            self._do_cancel_order_stock_sysid_sync, account, market, sysid, emit_events, seq,
        )
        return future.result()

    def cancel_order_stock_async_with_sync(
        self, account: Any, order_id: int, emit_events: Any, callback: Any = None,
    ) -> int:
        """异步撤单，对齐 xtquant 的 cancel_order_stock_async。"""
        import random
        seq = random.randint(100000, 999999)
        self._ensure_executor().submit(
            self._do_cancel_order_stock_async_sync, account, order_id, emit_events, callback, seq,
        )
        return seq

    def cancel_order_stock_sysid_async_with_sync(
        self, account: Any, market: Any, sysid: str, emit_events: Any, callback: Any = None,
    ) -> int:
        """按系统编号异步撤单，对齐 xtquant 的 cancel_order_stock_sysid_async。"""
        import random
        seq = random.randint(100000, 999999)
        self._ensure_executor().submit(
            self._do_cancel_order_stock_sysid_async_sync, account, market, sysid, emit_events, callback, seq,
        )
        return seq

    def _do_cancel_order_stock_sync(
        self, account: Any, order_id: int, emit_events: Any, seq: int,
    ) -> None:
        """在 executor 线程中执行撤单，对齐 xtquant 的 on_common_resp_callback。"""
        cancel_result, _events = self.cancel_order_stock(account, order_id)
        sync_reject_events = self.cancel_order_stock_sync_check(account, order_id)
        emit_events(sync_reject_events)
        callback = self._cancel_cbs.pop(seq, None)
        if callback:
            callback(cancel_result)

    def _do_cancel_order_stock_sysid_sync(
        self, account: Any, market: Any, sysid: str, emit_events: Any, seq: int,
    ) -> None:
        """在 executor 线程中执行按系统编号撤单。"""
        cancel_result, _events = self.cancel_order_stock_sysid(account, market, sysid)
        sync_reject_events = self.cancel_order_stock_sysid_sync_check(account, market, sysid)
        emit_events(sync_reject_events)
        callback = self._cancel_cbs.pop(seq, None)
        if callback:
            callback(cancel_result)

    def _do_cancel_order_stock_async_sync(
        self, account: Any, order_id: int, emit_events: Any, callback: Any, seq: int,
    ) -> None:
        """在 executor 线程中执行异步撤单。"""
        from .paper_trader import PaperCancelOrderResponse
        cancel_result, _events = self.cancel_order_stock(account, order_id)
        sync_reject_events = self.cancel_order_stock_sync_check(account, order_id)
        emit_events(sync_reject_events)
        if callable(callback):
            account_state = self._get_or_create_account_state(account)[0]
            order = self.query_stock_order(account, order_id)
            callback(PaperCancelOrderResponse(
                account_type=account_state.account_type,
                account_id=account_state.account_id,
                cancel_result=cancel_result,
                order_id=order_id,
                order_sysid=order.order_sysid if order else "",
                seq=seq,
                error_msg="" if cancel_result == 0 else "撤单失败",
            ))

    def _do_cancel_order_stock_sysid_async_sync(
        self, account: Any, market: Any, sysid: str, emit_events: Any, callback: Any, seq: int,
    ) -> None:
        """在 executor 线程中执行按系统编号异步撤单。"""
        from .paper_trader import PaperCancelOrderResponse
        cancel_result, _events = self.cancel_order_stock_sysid(account, market, sysid)
        sync_reject_events = self.cancel_order_stock_sysid_sync_check(account, market, sysid)
        emit_events(sync_reject_events)
        if callable(callback):
            account_state = self._get_or_create_account_state(account)[0]
            callback(PaperCancelOrderResponse(
                account_type=account_state.account_type,
                account_id=account_state.account_id,
                cancel_result=cancel_result,
                order_id=0,
                order_sysid=str(sysid),
                seq=seq,
                error_msg="" if cancel_result == 0 else "撤单失败",
            ))

    # ==================== 订单生命周期推进 ====================

    def tick(self, account_key: tuple[str, str], order_id: int) -> list[PaperEvent] | None:
        """推进一个活跃订单的一次成交，返回事件列表。

        返回 None 表示订单已终结或不存在，外部可停止对该订单的 tick。
        调用方应在等待 fill_interval_seconds 后再次调用。
        """
        with self._lock:
            account_state = self._accounts.get(account_key)
            if account_state is None:
                return None
            order_state = account_state.orders.get(order_id)
            if order_state is None:
                return None
            if not order_state.accepted:
                return None
            if order_state.order_status in self._constants.terminal_order_statuses:
                return None
            if order_state.cancel_requested:
                return self._apply_cancel_locked(account_state, order_state)

            remaining_volume = max(order_state.order_volume - order_state.traded_volume, 0)
            if remaining_volume <= 0:
                order_state.order_status = self._constants.order_succeeded
                order_state.status_msg = "已成"
                order_snapshot = self._build_order_snapshot_locked(order_state)
                return [PaperEvent("on_stock_order", order_snapshot)]

            round_index = order_state.trade_count + 1
            if self._last_fill_round(round_index):
                fill_volume = remaining_volume
            else:
                fill_volume = self._build_next_fill_volume(remaining_volume)
            if fill_volume <= 0:
                order_state.order_status = self._constants.order_junk
                order_state.status_msg = "废单"
                order_snapshot = self._build_order_snapshot_locked(order_state)
                return [PaperEvent("on_stock_order", order_snapshot)]

            trade_state = self._apply_fill_locked(account_state, order_state, fill_volume)
            trade_snapshot = self._build_trade_snapshot_locked(trade_state)
            order_snapshot = self._build_order_snapshot_locked(order_state)

        events = [
            PaperEvent("on_stock_trade", trade_snapshot),
            PaperEvent("on_stock_order", order_snapshot),
        ]
        return events

    def is_order_active(self, account_key: tuple[str, str], order_id: int) -> bool:
        """判断订单是否仍在活跃状态，需要继续 tick。"""
        with self._lock:
            account_state = self._accounts.get(account_key)
            if account_state is None:
                return False
            order_state = account_state.orders.get(order_id)
            if order_state is None:
                return False
            if not order_state.accepted:
                return False
            return order_state.order_status not in self._constants.terminal_order_statuses

    def get_active_order_ids(self, account_key: tuple[str, str]) -> list[int]:
        """获取指定账户下所有仍需 tick 的活跃订单 ID。"""
        with self._lock:
            account_state = self._accounts.get(account_key)
            if account_state is None:
                return []
            return [
                oid for oid, ostate in account_state.orders.items()
                if ostate.accepted and ostate.order_status not in self._constants.terminal_order_statuses
            ]

    # ==================== 查询接口 ====================

    def query_account_status(self) -> list[PaperAccountStatus]:
        """查询所有账户状态。"""
        with self._lock:
            return [self._build_account_status_snapshot(s) for s in self._accounts.values()]

    def query_account_infos(self) -> list[PaperAccountInfo]:
        """查询所有账户信息。"""
        with self._lock:
            return [self._build_account_info_snapshot(s) for s in self._accounts.values()]

    def query_stock_asset(self, account: Any) -> PaperAsset:
        """查询账户资金。"""
        state, _ = self._get_or_create_account_state(account)
        with self._lock:
            return self._build_asset_snapshot_locked(state)

    def query_stock_position(self, account: Any, stock_code: str) -> PaperPosition | None:
        """查询单只股票持仓。"""
        state, _ = self._get_or_create_account_state(account)
        normalized = _normalize_stock_code(stock_code)
        with self._lock:
            position_state = state.positions.get(normalized)
            if position_state is None:
                return None
            return self._build_position_snapshot_locked(state, position_state)

    def query_stock_positions(self, account: Any) -> list[PaperPosition]:
        """查询账户所有持仓。"""
        state, _ = self._get_or_create_account_state(account)
        with self._lock:
            return self._build_position_snapshots_locked(state)

    def query_stock_order(self, account: Any, order_id: int) -> PaperOrder | None:
        """查询单笔委托。"""
        state, _ = self._get_or_create_account_state(account)
        with self._lock:
            order_state = state.orders.get(_as_int(order_id, -1))
            if order_state is None:
                return None
            return self._build_order_snapshot_locked(order_state)

    def query_stock_orders(self, account: Any, cancelable_only: bool = False) -> list[PaperOrder]:
        """查询委托列表。"""
        state, _ = self._get_or_create_account_state(account)
        with self._lock:
            orders = list(state.orders.values())
        if cancelable_only:
            return [
                self._build_order_snapshot_locked(o)
                for o in orders if o.order_status in self._constants.cancelable_order_statuses
            ]
        return [self._build_order_snapshot_locked(o) for o in orders]

    def query_stock_trades(self, account: Any) -> list[PaperTrade]:
        """查询成交列表。"""
        state, _ = self._get_or_create_account_state(account)
        with self._lock:
            return [self._build_trade_snapshot_locked(t) for t in state.trades]

    # ==================== 内部方法 ====================

    def _next_order_id_locked(self) -> int:
        """生成下一个委托编号。"""
        self._order_seq += 1
        return self._order_seq

    def _build_order_sysid(self, order_id: int) -> str:
        """生成模拟的系统委托编号。"""
        return f"P{order_id:08d}"

    def _get_account_key(self, account: Any | None) -> tuple[str, str]:
        """从账户对象中提取 (account_type, account_id) 作为字典键。"""
        if account is None:
            return ("", "")
        if isinstance(account, tuple):
            if len(account) >= 2:
                return (_as_str(_resolve_account_type(account[0])), _as_str(account[1]))
            return (_as_str(_resolve_account_type(account[0])), "")
        account_type = self._read_field(account, "account_type", "STOCK")
        account_id = self._read_field(account, "account_id", "")
        return (_as_str(_resolve_account_type(account_type)), _as_str(account_id))

    def _read_field(self, value: Any, field_name: str, default: Any = None) -> Any:
        """从对象或字典中读取字段值。"""
        if isinstance(value, Mapping):
            return value.get(field_name, default)
        return getattr(value, field_name, default)

    def _clone_seed_positions(self) -> dict[str, _PositionState]:
        """从种子数据克隆初始持仓状态。"""
        result: dict[str, _PositionState] = {}
        for pos in self._seed.positions:
            result[pos.stock_code] = _PositionState(
                stock_code=pos.stock_code,
                volume=_as_int(pos.volume),
                can_use_volume=_as_int(pos.can_use_volume) if pos.can_use_volume is not None else _as_int(pos.volume),
                avg_price=_as_float(pos.avg_price),
                last_price=_as_float(pos.last_price) if pos.last_price is not None else _as_float(pos.avg_price),
                yesterday_volume=_as_int(pos.yesterday_volume) if pos.yesterday_volume is not None else 0,
            )
        return result

    def _create_account_state(self, account: Any | None) -> _AccountState:
        """根据账户对象和种子数据创建内部状态。"""
        account_type, account_id = self._get_account_key(account)
        return _AccountState(
            account_id=account_id,
            account_type=account_type,
            cash=self._seed.cash,
            frozen_cash=self._seed.frozen_cash,
            positions=self._clone_seed_positions(),
        )

    def _get_or_create_account_state(self, account: Any | None) -> tuple[_AccountState, tuple[str, str]]:
        """获取或创建账户内部状态。"""
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
        market_value = self._calculate_market_value_locked(state)
        total_asset = round(state.cash + state.frozen_cash + market_value, 2)
        return PaperAccountInfo(
            account_type=state.account_type,
            account_id=state.account_id,
            status=state.status,
            cash=round(state.cash, 2),
            frozen_cash=round(state.frozen_cash, 2),
            market_value=round(market_value, 2),
            total_asset=round(total_asset, 2),
            fetch_balance=round(state.cash, 2),
        )

    def _calculate_market_value_locked(self, state: _AccountState) -> float:
        """计算持仓总市值。"""
        market_value = 0.0
        for position_state in state.positions.values():
            last_price = self._resolve_last_price(position_state)
            market_value += round(position_state.volume * last_price, 2)
        return round(market_value, 2)

    def _build_asset_snapshot_locked(self, state: _AccountState) -> PaperAsset:
        """构造资金快照。"""
        market_value = self._calculate_market_value_locked(state)
        total_asset = round(state.cash + state.frozen_cash + market_value, 2)
        return PaperAsset(
            account_type=state.account_type,
            account_id=state.account_id,
            cash=round(state.cash, 2),
            frozen_cash=round(state.frozen_cash, 2),
            market_value=round(market_value, 2),
            total_asset=round(total_asset, 2),
            fetch_balance=round(state.cash, 2),
        )

    def _build_position_snapshot_locked(self, state: _AccountState, position_state: _PositionState) -> PaperPosition:
        """构造持仓快照。"""
        last_price = self._resolve_last_price(position_state)
        market_value = round(position_state.volume * last_price, 2)
        profit_rate = 0.0
        if position_state.avg_price > 0:
            profit_rate = round((last_price - position_state.avg_price) / position_state.avg_price, 4)
        return PaperPosition(
            account_type=state.account_type,
            account_id=state.account_id,
            stock_code=position_state.stock_code,
            volume=position_state.volume,
            can_use_volume=position_state.can_use_volume,
            open_price=round(position_state.avg_price, 4),
            market_value=round(market_value, 2),
            frozen_volume=position_state.frozen_volume,
            on_road_volume=position_state.on_road_volume,
            yesterday_volume=position_state.yesterday_volume,
            avg_price=round(position_state.avg_price, 4),
            direction=0,
            last_price=round(last_price, 4),
            profit_rate=round(profit_rate, 4),
            secu_account=state.account_id,
            instrument_name=self._resolve_instrument_name(position_state.stock_code),
        )

    def _build_position_snapshots_locked(self, state: _AccountState) -> list[PaperPosition]:
        """构造所有持仓快照。"""
        return [self._build_position_snapshot_locked(state, ps) for ps in state.positions.values()]

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
            price=order_state.price,
            traded_volume=order_state.traded_volume,
            traded_price=order_state.traded_price,
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
            traded_price=trade_state.traded_price,
            traded_volume=trade_state.traded_volume,
            traded_amount=trade_state.traded_amount,
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
        """解析纸面交易的执行价格。

        与真实柜台逻辑一致：
        - FIX_PRICE：直接用传入价。
        - LATEST_PRICE：取 xtdata 最新价，取不到回退传入价。
        - 沪市标的 (.SH) 的市价类型：传入价有效则用传入价，否则取涨停价。
        - 深市标的 (.SZ) 的市价类型（MARKET_PEER_PRICE_FIRST 等）：忽略传入价，直接用涨停价。
          涨停价 = 昨收 × 1.10，昨收通过 xtdata.get_market_data 拉取日线数据获取。
          取不到昨收时回退到最新价，再取不到回退到传入价。
        """
        normalized = _normalize_stock_code(stock_code)

        if price_type == self._constants.fix_price:
            return max(_as_float(order_price, 0.0), 0.0)

        if price_type == self._constants.latest_price:
            return self._fetch_latest_price(normalized, order_price)

        # 市价类型（MARKET_PEER_PRICE_FIRST / MARKET_SZ_INSTBUSI_RESTCANCEL / MARKET_SZ_FULL_OR_CANCEL 等）
        if self._is_sh_market(normalized):
            if _as_float(order_price, 0.0) > 0:
                return _as_float(order_price)
            # 沪市市价单未传价，取涨停价
            return self._fetch_upper_limit_price(normalized, order_price)

        # 深市及其他市场：市价单忽略传入价，直接用涨停价
        return self._fetch_upper_limit_price(normalized, order_price)

    def _fetch_upper_limit_price(self, stock_code: str, fallback: float) -> float:
        """获取涨停价（昨收 × 1.10），取不到回退到最新价再回退到 fallback。

        昨收通过 xtdata.get_market_data 拉取最近一个交易日的日线 close 字段。
        """
        yesterday_close = self._fetch_yesterday_close(stock_code)
        if yesterday_close > 0:
            return round(yesterday_close * 1.10, 2)
        # 昨收取不到，回退到最新价
        latest = self._fetch_latest_price(stock_code, fallback)
        if latest > 0:
            return latest
        return max(_as_float(fallback, 0.0), 0.0)

    def _fetch_yesterday_close(self, stock_code: str) -> float:
        """从 xtdata 历史数据拉取最近一个交易日的收盘价。"""
        xtdata = self._xtdata
        if xtdata is None or not callable(getattr(xtdata, "get_market_data", None)):
            return 0.0
        try:
            from datetime import datetime, timedelta
            # 往前取 10 个日历日覆盖周末和节假日
            end_time = datetime.now().strftime("%Y%m%d")
            start_time = (datetime.now() - timedelta(days=10)).strftime("%Y%m%d")
            data = xtdata.get_market_data(
                field_list=["close"],
                stock_list=[stock_code],
                period="1d",
                start_time=start_time,
                end_time=end_time,
                fill_data=False,
            )
            if data is not None and not (hasattr(data, "empty") and data.empty):
                # 取最后一条有效 close
                if hasattr(data, "iloc"):
                    closes = data["close"].dropna()
                else:
                    closes = data.get("close") if isinstance(data, dict) else None
                if hasattr(closes, "iloc") and len(closes) > 0:
                    return _as_float(closes.iloc[-1], 0.0)
                if isinstance(closes, (list, tuple)) and len(closes) > 0:
                    return _as_float(closes[-1], 0.0)
        except Exception:
            pass
        return 0.0

    def _fetch_latest_price(self, stock_code: str, fallback: float) -> float:
        """从 xtdata 获取最新价，取不到回退到 fallback。"""
        xtdata = self._xtdata
        if xtdata is not None and callable(getattr(xtdata, "get_full_tick", None)):
            try:
                tick_data = xtdata.get_full_tick([stock_code])
                if isinstance(tick_data, Mapping):
                    item = tick_data.get(stock_code)
                else:
                    item = getattr(tick_data, stock_code, None)
                if isinstance(item, Mapping):
                    candidate = _as_float(item.get("lastPrice"), 0.0)
                else:
                    candidate = _as_float(getattr(item, "lastPrice", 0.0), 0.0)
                if candidate > 0:
                    return candidate
            except Exception:
                pass
        return max(_as_float(fallback, 0.0), 0.0)

    @staticmethod
    def _is_sh_market(stock_code: str) -> bool:
        """判断证券代码是否为沪市标的。"""
        normalized = str(stock_code).strip().upper()
        return normalized.endswith(".SH")

    def _get_sellable_volume_locked(self, state: _AccountState, stock_code: str) -> int:
        """计算当前可卖数量。"""
        position_state = state.positions.get(stock_code)
        if position_state is None:
            return 0
        return min(position_state.volume, position_state.can_use_volume)

    def _check_buy_capacity_locked(self, state: _AccountState, order_volume: int, execution_price: float) -> bool:
        """判断当前现金是否足够买入。"""
        return state.cash >= round(order_volume * execution_price, 2)

    def _reserve_buy_cash_locked(self, state: _AccountState, order_volume: int, execution_price: float) -> float:
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
    ) -> tuple[int, list[PaperEvent]]:
        """创建一个废单并返回事件。"""
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
        return order_id, [
            PaperEvent("on_order_error", error_snapshot),
            PaperEvent("on_stock_order", order_snapshot),
        ]

    # 单笔订单最多撮合次数，超过后剩余数量一次性成交。
    _MAX_FILL_ROUNDS = 4

    def _last_fill_round(self, round_index: int) -> bool:
        """判断当前是否为最后一轮撮合。"""
        return round_index >= self._MAX_FILL_ROUNDS

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
                    volume=0, can_use_volume=0, avg_price=0.0,
                    last_price=fill_price, yesterday_volume=0,
                )
                account_state.positions[order_state.stock_code] = position_state
            total_volume = position_state.volume + fill_volume
            if total_volume <= 0:
                total_volume = fill_volume
            position_state.avg_price = round(
                (position_state.avg_price * position_state.volume + fill_price * fill_volume) / total_volume, 4,
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
                    volume=0, can_use_volume=0, avg_price=0.0,
                    last_price=fill_price, yesterday_volume=0,
                )
                account_state.positions[order_state.stock_code] = position_state
            position_state.frozen_volume = max(position_state.frozen_volume - fill_volume, 0)
            position_state.volume = max(position_state.volume - fill_volume, 0)
            # 成交后同步可卖数量，避免后续 _get_sellable_volume_locked 返回偏小值
            position_state.can_use_volume = min(position_state.can_use_volume, position_state.volume)
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

    def _apply_cancel_locked(self, account_state: _AccountState, order_state: _OrderState) -> list[PaperEvent]:
        """在持锁状态下应用撤单，返回事件列表。

        模拟真实柜台异步撤单的两种结果：
        - 已全部成交 → 撤单被拒，推送 on_cancel_error + 终态 on_stock_order(56)
        - 尚未全部成交 → 撤单成功，释放资源，推送终态 on_stock_order(53 部撤 或 54 已撤)
        """
        events: list[PaperEvent] = []
        remaining_volume = max(order_state.order_volume - order_state.traded_volume, 0)

        # 如果已经全部成交，撤单被柜台拒绝，与真实 on_cancel_error 一致
        if remaining_volume <= 0:
            order_state.cancel_requested = False
            cancel_error = PaperCancelError(
                account_type=account_state.account_type,
                account_id=account_state.account_id,
                order_id=order_state.order_id,
                market=0,
                order_sysid=order_state.order_sysid,
                error_id=DEFAULT_CANCEL_ERROR_ID,
                error_msg="委托已全部成交，无法撤单。",
                order_status=order_state.order_status,
            )
            events.append(PaperEvent("on_cancel_error", cancel_error))
            # 保序推送终态订单快照
            events.append(PaperEvent("on_stock_order", self._build_order_snapshot_locked(order_state)))
            return events

        # 撤单成功：更新状态并释放冻结资源
        if order_state.traded_volume > 0:
            order_state.order_status = self._constants.order_part_cancel
            order_state.status_msg = "部撤"
        else:
            order_state.order_status = self._constants.order_canceled
            order_state.status_msg = "已撤"

        if order_state.order_type == self._constants.stock_buy:
            remaining_cash = round(remaining_volume * order_state.price, 2)
            self._release_buy_cash_locked(account_state, remaining_cash)
        else:
            position_state = account_state.positions.get(order_state.stock_code)
            if position_state is not None:
                self._release_sell_volume_locked(position_state, remaining_volume)
                if position_state.volume <= 0 and position_state.frozen_volume <= 0:
                    account_state.positions.pop(order_state.stock_code, None)

        events.append(PaperEvent("on_stock_order", self._build_order_snapshot_locked(order_state)))
        return events

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


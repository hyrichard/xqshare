"""纸面交易模式测试。

本模块覆盖 paper trader 的核心场景：
1. PaperSimulator 的下单/撤单/查询基本逻辑。
2. TraderBridge 在 paper 模式下正确桥接到 PaperSimulator。
3. 股票买卖委托在部分成交后可被撤单，并能继续推进到终态。
4. 买单先冻结资金，避免并发委托重复占用。
5. 卖单先冻结持仓，避免连续卖单重复占用。
6. 服务端在 paper 模式下返回携带 PaperSimulator 的 TraderBridge。
"""

from __future__ import annotations

import json
import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import xqshare.server as server_module
from xqshare.paper_trader import PaperSeedPosition, PaperTraderSeed, PaperSimulator, PaperEvent
from xqshare.server import AccountLevel, XtQuantService, CallbackManager

# 确保 server 模块的 logger 已初始化
server_module._init_logging("WARNING")


class _EventRecorder:
    """记录纸面交易回调，便于断言事件顺序。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.events: list[tuple[str, tuple]] = []
        self.first_trade_event = threading.Event()

    def _append(self, event_name: str, *payload) -> None:
        with self._lock:
            self.events.append((event_name, payload))

    def snapshot(self) -> list[tuple[str, tuple]]:
        with self._lock:
            return list(self.events)

    def on_connected(self) -> None:
        self._append("on_connected")

    def on_account_status(self, status) -> None:
        self._append("on_account_status", status.status)

    def on_stock_asset(self, asset) -> None:
        self._append("on_stock_asset", asset.cash, asset.frozen_cash, asset.market_value, asset.total_asset)

    def on_stock_position(self, position) -> None:
        self._append(
            "on_stock_position",
            position.stock_code,
            position.volume,
            position.can_use_volume,
            position.last_price,
        )

    def on_stock_order(self, order) -> None:
        self._append(
            "on_stock_order",
            order.order_id,
            order.order_status,
            order.traded_volume,
            order.status_msg,
        )

    def on_stock_trade(self, trade) -> None:
        self._append(
            "on_stock_trade",
            trade.order_id,
            trade.traded_volume,
            trade.traded_price,
        )
        self.first_trade_event.set()

    def on_order_error(self, error) -> None:
        self._append("on_order_error", error.order_id, error.error_msg)

    def on_cancel_error(self, error) -> None:
        self._append("on_cancel_error", error.order_id, error.error_msg)


def _wait_until(predicate, timeout: float = 2.0, interval: float = 0.01) -> bool:
    """等待条件成立，避免后台线程带来的时序抖动。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def _create_paper_bridge(xtdata, seed, fill_interval=0.1, session_id=1):
    """创建一个 paper 模式的 TraderBridge，带回调桥接。

    回调事件经过完整的 CallbackManager.invoke_event -> dispatcher 路径。
    注意：不启动撮合线程，测试中手动调用 tick 推进订单。
    """
    from xqshare.server import TraderBridge, TraderCallbackAdapter
    sim = PaperSimulator(
        xtdata_module=xtdata,
        seed=seed,
        fill_interval_seconds=fill_interval,
    )
    callback_manager = CallbackManager()
    bridge = TraderBridge(
        trader=SimpleNamespace(userdata_path=None, session_id=session_id),
        userdata_path=None,
        session_id=session_id,
        client_info_getter=lambda: "test",
        permission_checker=None,
        account_level=AccountLevel.PREMIUM,
        callback_manager=callback_manager,
        paper_simulator=sim,
    )
    # 用真实的 dispatcher 函数注册回调，走完整 invoke_event 路径
    recorder = _EventRecorder()
    binding_id = "test_cb"

    def dispatcher(binding_id, event_name, *args, **kwargs):
        """真实回调派发：由 CallbackManager.invoke_event(binding_id, event_name, *args, **kwargs) 调用。"""
        handler = getattr(recorder, event_name, None)
        if callable(handler):
            handler(*args, **kwargs)

    callback_manager.register(
        binding_id,
        dispatcher=dispatcher,
        kind="xttrader_callback",
        client_info="test",
    )
    adapter = TraderCallbackAdapter(binding_id, callback_manager)
    bridge._callback_binding_id = binding_id
    bridge._callback_adapter = adapter

    return bridge, recorder, sim


def test_paper_trader_partial_fill_then_cancel() -> None:
    """验证纸面交易可在部分成交后撤单，并继续推送终态。"""

    xtdata = MagicMock()
    xtdata.get_full_tick.return_value = {"000001.SZ": {"lastPrice": 10.0}}
    xtdata.get_instrument_detail.return_value = {"InstrumentName": "平安银行"}

    bridge, recorder, sim = _create_paper_bridge(
        xtdata, PaperTraderSeed(cash=100.0), fill_interval=0.1
    )

    account = SimpleNamespace(account_id="A1", account_type="STOCK")
    bridge._paper_connect()
    bridge._paper_subscribe(account)

    order_id = bridge._paper_order_stock(account, "000001.SZ", 23, 10, 11, 10.0)
    assert order_id == 1

    # 手动 tick 推进撮合（避免依赖后台线程时序）
    account_key = sim._get_account_key(account)
    first_tick_events = sim.tick(account_key, order_id)
    assert first_tick_events is not None
    bridge._emit_paper_events(first_tick_events)

    assert recorder.first_trade_event.wait(timeout=2.0)
    cancel_result = bridge._paper_cancel_order_stock(account, order_id)
    assert cancel_result == 0

    # 继续 tick 直到撤单完成
    _wait_until(lambda: not sim.is_order_active(account_key, order_id), timeout=2.0)
    cancel_tick_events = sim.tick(account_key, order_id)
    if cancel_tick_events is not None:
        bridge._emit_paper_events(cancel_tick_events)

    order = bridge._paper_query_stock_order(account, order_id)
    assert order is not None
    assert order.order_status == 53
    assert 0 < order.traded_volume < order.order_volume

    trades = bridge._paper_query_stock_trades(account)
    assert trades
    assert sum(trade.traded_volume for trade in trades) == order.traded_volume

    asset = bridge._paper_query_stock_asset(account)
    position = bridge._paper_query_stock_position(account, "000001.SZ")
    assert position is not None
    assert position.instrument_name == "平安银行"
    assert asset.total_asset == 100.0
    assert asset.cash + asset.market_value == 100.0
    assert asset.cash < 100.0
    assert xtdata.get_full_tick.called
    assert xtdata.get_instrument_detail.called

    events = recorder.snapshot()
    statuses = [
        payload[1]
        for event_name, payload in events
        if event_name == "on_stock_order"
    ]
    assert 50 in statuses
    assert 55 in statuses


def test_paper_trader_reserves_cash_before_buy_fill() -> None:
    """验证买单会先冻结资金，避免并发委托重复占用同一笔现金。

    直接使用 PaperSimulator 测试，避免后台撮合线程干扰时序。
    """

    xtdata = MagicMock()
    xtdata.get_full_tick.return_value = {"000001.SZ": {"lastPrice": 10.0}}
    xtdata.get_instrument_detail.return_value = {"InstrumentName": "平安银行"}

    sim = PaperSimulator(
        xtdata_module=xtdata,
        seed=PaperTraderSeed(cash=100.0),
        fill_interval_seconds=2.0,
    )

    account = SimpleNamespace(account_id="A1", account_type="STOCK")
    sim.subscribe_account(account)

    first_order_id, _ = sim.order_stock(account, "000001.SZ", 23, 6, 11, 10.0)
    second_order_id, _ = sim.order_stock(account, "000001.SZ", 23, 6, 11, 10.0)

    first_order = sim.query_stock_order(account, first_order_id)
    second_order = sim.query_stock_order(account, second_order_id)
    asset = sim.query_stock_asset(account)

    assert first_order is not None
    assert second_order is not None
    assert first_order.order_status == 50
    assert second_order.order_status == 57
    assert asset.cash == 40.0
    assert asset.frozen_cash == 60.0
    assert asset.total_asset == 100.0


def test_paper_trader_reserves_position_before_sell_fill() -> None:
    """验证卖单会先冻结可用持仓，避免连续卖单重复占用同一份股票。

    直接使用 PaperSimulator 测试，避免后台撮合线程干扰时序。
    """

    xtdata = MagicMock()
    xtdata.get_full_tick.return_value = {"000001.SZ": {"lastPrice": 10.0}}
    xtdata.get_instrument_detail.return_value = {"InstrumentName": "平安银行"}

    sim = PaperSimulator(
        xtdata_module=xtdata,
        seed=PaperTraderSeed(
            cash=0.0,
            positions=(
                PaperSeedPosition(
                    stock_code="000001.SZ",
                    volume=10,
                    avg_price=10.0,
                ),
            ),
        ),
        fill_interval_seconds=2.0,
    )

    account = SimpleNamespace(account_id="A1", account_type="STOCK")
    sim.subscribe_account(account)

    first_order_id, _ = sim.order_stock(account, "000001.SZ", 24, 6, 11, 10.0)
    second_order_id, _ = sim.order_stock(account, "000001.SZ", 24, 6, 11, 10.0)

    first_order = sim.query_stock_order(account, first_order_id)
    second_order = sim.query_stock_order(account, second_order_id)
    position = sim.query_stock_position(account, "000001.SZ")
    asset = sim.query_stock_asset(account)

    assert first_order is not None
    assert second_order is not None
    assert position is not None
    assert first_order.order_status == 50
    assert second_order.order_status == 57
    assert position.volume == 10
    assert position.can_use_volume == 4
    assert position.frozen_volume == 6
    assert asset.cash == 0.0
    assert asset.frozen_cash == 0.0
    assert asset.total_asset == 100.0


def test_server_create_trader_uses_paper_mode(monkeypatch) -> None:
    """验证服务端会在 paper 模式下返回携带 PaperSimulator 的 TraderBridge。"""

    monkeypatch.setenv("XQSHARE_TRADER_MODE", "paper")
    monkeypatch.setenv(
        "XQSHARE_PAPER_TRADER_SEED",
        json.dumps(
            {
                "cash": 888.0,
                "positions": {
                    "000001.SZ": {
                        "volume": 100,
                        "avg_price": 9.5,
                    }
                },
            },
            ensure_ascii=False,
        ),
    )
    monkeypatch.setenv("XQSHARE_PAPER_TRADER_FILL_INTERVAL_SECONDS", "0.01")

    service = XtQuantService()
    service._conn = MagicMock()
    service._conn.peer = "127.0.0.1:12345"
    monkeypatch.setattr(server_module, "logger", MagicMock(), raising=False)
    monkeypatch.setattr(server_module, "api_logger", MagicMock(), raising=False)
    service.on_connect(service._conn)
    service._authenticated = True
    service._account_level = AccountLevel.PREMIUM
    service._client_info = "test-client"
    service._xtdata = MagicMock()
    service._xtconstant = MagicMock()
    monkeypatch.setattr(XtQuantService, "_permission_checker", None, raising=False)

    bridge = service.exposed_create_trader("C:\\QMT\\userdata_mini", 123)

    # paper 模式下 bridge 应持有 PaperSimulator
    assert bridge.is_paper_mode
    assert isinstance(bridge._paper, PaperSimulator)
    assert bridge.userdata_path == "C:\\QMT\\userdata_mini"
    assert bridge.session_id == 123
    assert bridge._paper.seed.cash == 888.0
    assert bridge._paper.seed.positions[0].stock_code == "000001.SZ"
    # paper 模式下不应调用真实 XtQuantTrader
    assert bridge._trader.__class__.__name__ == "PaperTraderStub"


def test_simulator_tick_returns_none_for_finished_order() -> None:
    """验证 PaperSimulator.tick() 对已完成订单返回 None。"""

    xtdata = MagicMock()
    xtdata.get_full_tick.return_value = {"000001.SZ": {"lastPrice": 10.0}}
    xtdata.get_instrument_detail.return_value = {"InstrumentName": "平安银行"}

    sim = PaperSimulator(
        xtdata_module=xtdata,
        seed=PaperTraderSeed(cash=1000.0),
        fill_interval_seconds=0.01,
    )

    account = SimpleNamespace(account_id="A1", account_type="STOCK")
    sim.subscribe_account(account)

    order_id, events = sim.order_stock(account, "000001.SZ", 23, 5, 11, 10.0)
    account_key = sim._get_account_key(account)

    # tick 直到订单完成
    for _ in range(20):
        result = sim.tick(account_key, order_id)
        if result is None:
            break
        time.sleep(0.01)

    # 再 tick 应返回 None
    assert sim.tick(account_key, order_id) is None
    assert not sim.is_order_active(account_key, order_id)

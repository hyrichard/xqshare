"""纸面交易模式测试。

本模块覆盖 paper trader 的两个核心场景：
1. 只替换 xttrader，不影响 xtdata 的真实读取路径。
2. 股票买卖委托在部分成交后可被撤单，并能继续推进到终态。
"""

from __future__ import annotations

import json
import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import xqshare.server as server_module
from xqshare.paper_trader import PaperSeedPosition, PaperTraderSeed, PaperXtQuantTrader
from xqshare.server import AccountLevel, XtQuantService, XtQuantTrader


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


def test_paper_trader_partial_fill_then_cancel() -> None:
    """验证纸面交易可在部分成交后撤单，并继续推送终态。"""

    xtdata = MagicMock()
    xtdata.get_full_tick.return_value = {"000001.SZ": {"lastPrice": 10.0}}
    xtdata.get_instrument_detail.return_value = {"InstrumentName": "平安银行"}

    trader = PaperXtQuantTrader(
        userdata_path=None,
        session_id=1,
        xtdata_module=xtdata,
        seed=PaperTraderSeed(cash=100.0),
        fill_interval_seconds=0.1,
    )
    recorder = _EventRecorder()
    trader.register_callback(recorder)

    account = SimpleNamespace(account_id="A1", account_type="STOCK")
    trader.start()
    trader.connect()
    trader.subscribe(account)

    order_id = trader.order_stock(account, "000001.SZ", 23, 10, 11, 10.0)
    assert order_id == 1

    assert recorder.first_trade_event.wait(timeout=2.0)
    cancel_result = trader.cancel_order_stock(account, order_id)
    assert cancel_result == 0

    assert _wait_until(
        lambda: any(
            event_name == "on_stock_order" and payload[1] == 53
            for event_name, payload in recorder.snapshot()
        ),
        timeout=2.0,
    )

    order = trader.query_stock_order(account, order_id)
    assert order is not None
    assert order.order_status == 53
    assert 0 < order.traded_volume < order.order_volume

    trades = trader.query_stock_trades(account)
    assert trades
    assert sum(trade.traded_volume for trade in trades) == order.traded_volume

    asset = trader.query_stock_asset(account)
    position = trader.query_stock_position(account, "000001.SZ")
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
    assert 52 in statuses
    assert 53 in statuses


def test_paper_trader_reserves_cash_before_buy_fill() -> None:
    """验证买单会先冻结资金，避免并发委托重复占用同一笔现金。"""

    xtdata = MagicMock()
    xtdata.get_full_tick.return_value = {"000001.SZ": {"lastPrice": 10.0}}
    xtdata.get_instrument_detail.return_value = {"InstrumentName": "平安银行"}

    trader = PaperXtQuantTrader(
        userdata_path=None,
        session_id=2,
        xtdata_module=xtdata,
        seed=PaperTraderSeed(cash=100.0),
        fill_interval_seconds=2.0,
    )
    trader.start()
    trader.connect()

    account = SimpleNamespace(account_id="A1", account_type="STOCK")
    trader.subscribe(account)

    first_order_id = trader.order_stock(account, "000001.SZ", 23, 6, 11, 10.0)
    second_order_id = trader.order_stock(account, "000001.SZ", 23, 6, 11, 10.0)

    first_order = trader.query_stock_order(account, first_order_id)
    second_order = trader.query_stock_order(account, second_order_id)
    asset = trader.query_stock_asset(account)

    assert first_order is not None
    assert second_order is not None
    assert first_order.order_status == 50
    assert second_order.order_status == 57
    assert asset.cash == 40.0
    assert asset.frozen_cash == 60.0
    assert asset.total_asset == 100.0


def test_paper_trader_reserves_position_before_sell_fill() -> None:
    """验证卖单会先冻结可用持仓，避免连续卖单重复占用同一份股票。"""

    xtdata = MagicMock()
    xtdata.get_full_tick.return_value = {"000001.SZ": {"lastPrice": 10.0}}
    xtdata.get_instrument_detail.return_value = {"InstrumentName": "平安银行"}

    trader = PaperXtQuantTrader(
        userdata_path=None,
        session_id=3,
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
    trader.start()
    trader.connect()

    account = SimpleNamespace(account_id="A1", account_type="STOCK")
    trader.subscribe(account)

    first_order_id = trader.order_stock(account, "000001.SZ", 24, 6, 11, 10.0)
    second_order_id = trader.order_stock(account, "000001.SZ", 24, 6, 11, 10.0)

    first_order = trader.query_stock_order(account, first_order_id)
    second_order = trader.query_stock_order(account, second_order_id)
    position = trader.query_stock_position(account, "000001.SZ")
    asset = trader.query_stock_asset(account)

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
    """验证服务端会在 paper 模式下返回纸面交易对象。"""

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

    before_call_count = getattr(XtQuantTrader, "call_count", 0)
    bridge = service.exposed_create_trader("C:\\QMT\\userdata_mini", 123)

    assert isinstance(bridge._trader, PaperXtQuantTrader)
    assert bridge.userdata_path == "C:\\QMT\\userdata_mini"
    assert bridge.session_id == 123
    assert bridge._trader._xtdata is service._xtdata
    assert bridge._trader._seed.cash == 888.0
    assert bridge._trader._seed.positions[0].stock_code == "000001.SZ"
    assert getattr(XtQuantTrader, "call_count", 0) == before_call_count

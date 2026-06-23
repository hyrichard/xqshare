"""
XtQuant Share (xqshare) Client Tests
"""

import pytest
from unittest.mock import MagicMock, Mock, patch

from xqshare.client import (
    CallbackError,
    CallbackRegistry,
    ReconnectPolicy,
    RemoteModule,
    XtQuantRemote,
)


class TestReconnectPolicy:
    def test_base_delay(self):
        policy = ReconnectPolicy(max_retries=5, base_delay=1, backoff_factor=2)
        assert policy.get_delay(0) == 1

    def test_exponential_backoff(self):
        policy = ReconnectPolicy(max_retries=5, base_delay=1, backoff_factor=2)
        assert policy.get_delay(1) == 2
        assert policy.get_delay(2) == 4

    def test_max_delay(self):
        policy = ReconnectPolicy(max_retries=5, base_delay=1, max_delay=10, backoff_factor=2)
        assert policy.get_delay(10) == 10


class TestCallbackRegistry:
    def test_register_and_invoke(self):
        registry = CallbackRegistry()
        callback = Mock(return_value="ok")

        callback_id = registry.register(callback, kind="xtdata")
        result = registry.invoke(callback_id, 1, foo="bar")

        assert result == "ok"
        callback.assert_called_once_with(1, foo="bar")

    def test_invoke_one_shot_callback(self):
        registry = CallbackRegistry()
        callback = Mock(return_value="done")

        callback_id = registry.register(callback, kind="async", one_shot=True)
        assert registry.invoke(callback_id, "payload") == "done"

        with pytest.raises(CallbackError):
            registry.invoke(callback_id, "payload")

    def test_invoke_event_dispatch(self):
        registry = CallbackRegistry()
        callback = MagicMock()
        callback.on_stock_order = Mock(return_value="event-ok")

        callback_id = registry.register(callback, kind="xttrader")
        result = registry.invoke_event(callback_id, "on_stock_order", 1, 2)

        assert result == "event-ok"
        callback.on_stock_order.assert_called_once_with(1, 2)


class TestRemoteModule:
    def test_attribute_access(self):
        mock_client = Mock()
        mock_client._ensure_connected = Mock()
        mock_client._should_reconnect = Mock(return_value=False)
        mock_client._conn = Mock()

        mock_module = Mock()
        mock_module.test_func = Mock(return_value="test_result")
        mock_client._conn.root.get_xtdata = Mock(return_value=mock_module)

        remote = RemoteModule(mock_client, 'xtdata')
        assert remote.test_func() == "test_result"


class TestXtQuantRemote:
    def _build_conn(self):
        conn = MagicMock()
        conn.root.authenticate.return_value = {"success": True, "level": "standard"}
        conn.root.heartbeat.return_value = "pong"
        conn.root.get_xtdata.return_value = MagicMock()
        conn.root.get_xttrader.return_value = MagicMock()
        conn.root.get_xttype.return_value = MagicMock()
        conn.root.get_xtconstant.return_value = MagicMock()
        conn.root.get_xtview.return_value = MagicMock()
        return conn

    @patch('xqshare.client.BgServingThread')
    @patch('xqshare.client.rpyc.connect')
    def test_connect_with_auth(self, mock_connect, _bg_thread):
        mock_conn = self._build_conn()
        mock_connect.return_value = mock_conn

        client = XtQuantRemote(host="localhost", port=18812, client_secret="secret", heartbeat_interval=0)

        assert client._connected is True
        assert client._account_level == "standard"
        mock_conn.root.authenticate.assert_called_once()

    @patch('xqshare.client.BgServingThread')
    @patch('xqshare.client.rpyc.connect')
    def test_xtdata_subscribe_bridge_and_unsubscribe(self, mock_connect, _bg_thread):
        mock_conn = self._build_conn()
        mock_conn.root.subscribe_xtdata_bridge.return_value = 42
        mock_conn.root.unsubscribe_xtdata_bridge.return_value = True
        mock_connect.return_value = mock_conn

        client = XtQuantRemote(host="localhost", port=18812, client_secret="secret", heartbeat_interval=0)
        callback = Mock()

        seq = client.xtdata.subscribe_quote("000001.SZ", period="tick", callback=callback)
        assert seq == 1

        call_args = mock_conn.root.subscribe_xtdata_bridge.call_args[0]
        assert call_args[0] == "subscribe_quote"
        callback_id = call_args[3]
        dispatcher = call_args[4]

        dispatcher(callback_id, {"000001.SZ": [{"lastPrice": 10.5}]})
        callback.assert_called_once_with({"000001.SZ": [{"lastPrice": 10.5}]})

        client.xtdata.unsubscribe_quote(seq)
        mock_conn.root.unsubscribe_xtdata_bridge.assert_called_once_with(42)

    @patch('xqshare.client.BgServingThread')
    @patch('xqshare.client.rpyc.connect')
    def test_formula_subscription_preserves_request_id_and_translates_followup_calls(self, mock_connect, _bg_thread):
        conn1 = self._build_conn()
        conn1.root.subscribe_xtdata_bridge.return_value = 900
        conn1.root.unsubscribe_xtdata_bridge.return_value = True
        formula_module_1 = conn1.root.get_xtdata.return_value
        formula_module_1.get_formula_result.return_value = {"value": 1}

        conn2 = self._build_conn()
        conn2.root.subscribe_xtdata_bridge.return_value = 901
        conn2.root.unsubscribe_xtdata_bridge.return_value = True
        formula_module_2 = conn2.root.get_xtdata.return_value
        formula_module_2.get_formula_result.return_value = {"value": 2}

        mock_connect.side_effect = [conn1, conn2]

        client = XtQuantRemote(host="localhost", port=18812, client_secret="secret", heartbeat_interval=0)
        request_id = client.xtdata.subscribe_formula(
            "my_formula",
            "000001.SZ",
            "1d",
            callback=Mock(),
        )
        assert request_id == 900

        client.reconnect()

        result = client.xtdata.get_formula_result(request_id)
        assert result == {"value": 2}
        formula_module_2.get_formula_result.assert_called_once_with(901)

        client.xtdata.unsubscribe_formula(request_id)
        conn2.root.unsubscribe_xtdata_bridge.assert_called_once_with(901)

    @patch('xqshare.client.BgServingThread')
    @patch('xqshare.client.rpyc.connect')
    def test_formula_unsubscribe_accepts_request_id_keyword(self, mock_connect, _bg_thread):
        mock_conn = self._build_conn()
        mock_conn.root.subscribe_xtdata_bridge.return_value = 900
        mock_conn.root.unsubscribe_xtdata_bridge.return_value = True
        mock_connect.return_value = mock_conn

        client = XtQuantRemote(host="localhost", port=18812, client_secret="secret", heartbeat_interval=0)
        request_id = client.xtdata.subscribe_formula(
            "my_formula",
            "000001.SZ",
            "1d",
            callback=Mock(),
        )

        result = client.xtdata.unsubscribe_formula(request_id=request_id)
        assert result is True
        mock_conn.root.unsubscribe_xtdata_bridge.assert_called_once_with(900)
        assert request_id not in client._subscriptions

    @patch('xqshare.client.BgServingThread')
    @patch('xqshare.client.rpyc.connect')
    def test_xtdata_subscribe_queue_bridge_preserves_trailing_kwargs(self, mock_connect, _bg_thread):
        mock_conn = self._build_conn()
        mock_conn.root.subscribe_xtdata_bridge.return_value = 43
        mock_conn.root.unsubscribe_xtdata_bridge.return_value = True
        mock_connect.return_value = mock_conn

        client = XtQuantRemote(host="localhost", port=18812, client_secret="secret", heartbeat_interval=0)
        callback = Mock(return_value=None)

        seq = client.xtdata.subscribe_l2thousand_queue("000001.SZ", callback, 2, 11.3)
        assert seq == 1

        call_args = mock_conn.root.subscribe_xtdata_bridge.call_args[0]
        assert call_args[0] == "subscribe_l2thousand_queue"
        assert call_args[1] == ("000001.SZ",)
        assert call_args[2] == {"gear_num": 2, "price": 11.3}

        callback_id = call_args[3]
        dispatcher = call_args[4]
        assert dispatcher(callback_id, {"000001.SZ": [{"lastPrice": 9.9}]}) is None
        callback.assert_called_once_with({"000001.SZ": [{"lastPrice": 9.9}]})

    @patch('xqshare.client.BgServingThread')
    @patch('xqshare.client.rpyc.connect')
    def test_trader_callback_bridge_and_async_bridge(self, mock_connect, _bg_thread):
        mock_conn = self._build_conn()
        trader_remote = MagicMock()
        trader_remote.userdata_path = "C:\\QMT\\userdata_mini"
        trader_remote.session_id = 123
        trader_remote.invoke_async_bridge.return_value = "async-started"
        mock_conn.root.create_trader.return_value = trader_remote
        mock_connect.return_value = mock_conn

        client = XtQuantRemote(host="localhost", port=18812, client_secret="secret", heartbeat_interval=0)
        trader = client.create_trader("C:\\QMT\\userdata_mini", 123)

        callback_obj = MagicMock()
        callback_obj.on_stock_order = Mock()
        trader.register_callback(callback_obj)

        register_args = trader_remote.register_callback_bridge.call_args[0]
        binding_id = register_args[0]
        event_dispatcher = register_args[1]
        event_dispatcher(binding_id, "on_stock_order", {"order_id": 1})
        callback_obj.on_stock_order.assert_called_once_with({"order_id": 1})

        async_callback = Mock(return_value="done")
        result = trader.query_stock_positions_async("account", callback=async_callback)
        assert result == "async-started"

        async_args = trader_remote.invoke_async_bridge.call_args[0]
        assert async_args[0] == "query_stock_positions_async"
        async_callback_id = async_args[3]
        async_dispatcher = async_args[4]
        assert async_dispatcher(async_callback_id, {"positions": []}) == "done"
        async_callback.assert_called_once_with({"positions": []})

    @patch('xqshare.client.BgServingThread')
    @patch('xqshare.client.rpyc.connect')
    def test_trader_async_bridge_preserves_optional_args_after_callback(self, mock_connect, _bg_thread):
        mock_conn = self._build_conn()
        trader_remote = MagicMock()
        trader_remote.userdata_path = "C:\\QMT\\userdata_mini"
        trader_remote.session_id = 123
        trader_remote.invoke_async_bridge.return_value = "async-started"
        mock_conn.root.create_trader.return_value = trader_remote
        mock_connect.return_value = mock_conn

        client = XtQuantRemote(host="localhost", port=18812, client_secret="secret", heartbeat_interval=0)
        trader = client.create_trader("C:\\QMT\\userdata_mini", 123)

        async_callback = Mock(return_value="done")
        result = trader.query_stock_orders_async("account", async_callback, False)
        assert result == "async-started"

        async_args = trader_remote.invoke_async_bridge.call_args[0]
        assert async_args[0] == "query_stock_orders_async"
        assert async_args[1] == ("account",)
        assert async_args[2] == {"cancelable_only": False}

        async_callback_id = async_args[3]
        async_dispatcher = async_args[4]
        assert async_dispatcher(async_callback_id, {"orders": []}) == "done"
        async_callback.assert_called_once_with({"orders": []})

    @patch('xqshare.client.BgServingThread')
    @patch('xqshare.client.rpyc.connect')
    def test_reconnect_restores_subscriptions_and_traders(self, mock_connect, _bg_thread):
        conn1 = self._build_conn()
        conn1.root.subscribe_xtdata_bridge.return_value = 11
        trader_remote_1 = MagicMock()
        trader_remote_1.userdata_path = "C:\\QMT\\userdata_mini"
        trader_remote_1.session_id = 7
        conn1.root.create_trader.return_value = trader_remote_1

        conn2 = self._build_conn()
        conn2.root.subscribe_xtdata_bridge.return_value = 22
        trader_remote_2 = MagicMock()
        trader_remote_2.userdata_path = "C:\\QMT\\userdata_mini"
        trader_remote_2.session_id = 7
        conn2.root.create_trader.return_value = trader_remote_2

        mock_connect.side_effect = [conn1, conn2]

        client = XtQuantRemote(host="localhost", port=18812, client_secret="secret", heartbeat_interval=0)
        client.xtdata.subscribe_quote("000001.SZ", period="tick", callback=Mock())

        trader = client.create_trader("C:\\QMT\\userdata_mini", 7)
        trader.start()
        trader.connect()
        trader.subscribe("account")
        trader.register_callback(MagicMock())

        client.reconnect()

        assert client._subscriptions[1].server_seq == 22
        conn2.root.create_trader.assert_called_once_with("C:\\QMT\\userdata_mini", 7)
        trader_remote_2.start.assert_called_once()
        trader_remote_2.connect.assert_called_once()
        trader_remote_2.subscribe.assert_called_once_with("account")
        trader_remote_2.register_callback_bridge.assert_called_once()

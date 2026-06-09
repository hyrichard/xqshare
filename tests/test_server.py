"""
XtQuant Share (xqshare) Server Tests
"""

from unittest.mock import MagicMock, Mock, patch

import pytest

from xqshare.server import (
    AuthError,
    CallbackManager,
    TraderBridge,
    XtQuantService,
    _init_logging,
    _summarize_result,
    create_ssl_context,
)

_init_logging("WARNING")


class TestSummarizeResult:
    def test_none(self):
        assert _summarize_result(None) == "None"

    def test_int(self):
        assert _summarize_result(42) == "42"

    def test_list(self):
        assert _summarize_result([1, 2, 3]) == "list[len=3]"


class TestCallbackManager:
    def test_register_and_invoke(self):
        manager = CallbackManager()
        dispatcher = Mock(return_value="ok")

        manager.register("cb1", dispatcher, "xtdata", "client1")
        assert manager.invoke("cb1", 1, 2) == "ok"
        dispatcher.assert_called_once_with("cb1", 1, 2)

    def test_invoke_event(self):
        manager = CallbackManager()
        dispatcher = Mock(return_value="event-ok")

        manager.register("binding1", dispatcher, "xttrader", "client1")
        assert manager.invoke_event("binding1", "on_stock_trade", {"trade_id": 1}) == "event-ok"
        dispatcher.assert_called_once_with("binding1", "on_stock_trade", {"trade_id": 1})

    def test_one_shot_unregister(self):
        manager = CallbackManager()
        dispatcher = Mock(return_value="done")

        manager.register("once", dispatcher, "async", "client1", one_shot=True)
        assert manager.invoke("once", "payload") == "done"
        assert manager.invoke("once", "payload") is False

    def test_clear_client_callbacks(self):
        manager = CallbackManager()
        manager.register("id1", Mock(), "xtdata", "client1")
        manager.register("id2", Mock(), "xtdata", "client2")

        removed = manager.clear_client_callbacks("client1")
        assert removed == ["id1"]
        callbacks = manager.list_callbacks()
        assert "id1" not in callbacks
        assert "id2" in callbacks


class TestTraderBridge:
    def test_register_callback_bridge_dispatches_events(self):
        trader = MagicMock()
        dispatcher = Mock(return_value="ok")
        manager = CallbackManager()
        bridge = TraderBridge(trader, "path", 1, lambda: "client1", None, None, manager)

        bridge.register_callback_bridge("binding1", dispatcher)
        adapter = trader.register_callback.call_args[0][0]
        assert adapter.on_stock_order({"order_id": 1}) == "ok"
        dispatcher.assert_called_once_with("binding1", "on_stock_order", {"order_id": 1})

    def test_register_callback_bridge_dispatches_async_order_response(self):
        trader = MagicMock()
        dispatcher = Mock(return_value="ok")
        manager = CallbackManager()
        bridge = TraderBridge(trader, "path", 1, lambda: "client1", None, None, manager)

        bridge.register_callback_bridge("binding1", dispatcher)
        adapter = trader.register_callback.call_args[0][0]
        assert adapter.on_order_stock_async_response({"seq": 1}) == "ok"
        dispatcher.assert_called_once_with("binding1", "on_order_stock_async_response", {"seq": 1})

    def test_register_callback_bridge_falls_back_for_other_on_events(self):
        trader = MagicMock()
        dispatcher = Mock(return_value="ok")
        manager = CallbackManager()
        bridge = TraderBridge(trader, "path", 1, lambda: "client1", None, None, manager)

        bridge.register_callback_bridge("binding1", dispatcher)
        adapter = trader.register_callback.call_args[0][0]
        assert adapter.on_stock_asset({"cash": 1}) == "ok"
        dispatcher.assert_called_once_with("binding1", "on_stock_asset", {"cash": 1})

    @pytest.mark.parametrize(
        ("method_name", "payload"),
        [
            ("on_connected", ()),
            ("on_stock_asset", ({"cash": 1},)),
            ("on_stock_position", ({"stock_code": "000001.SZ"},)),
            ("on_cancel_order_stock_async_response", ({"seq": 2},)),
            ("on_smt_appointment_async_response", ({"seq": 3},)),
            ("on_bank_transfer_async_response", ({"seq": 4},)),
        ],
    )
    def test_register_callback_bridge_dispatches_remaining_documented_events(self, method_name, payload):
        trader = MagicMock()
        dispatcher = Mock(return_value="ok")
        manager = CallbackManager()
        bridge = TraderBridge(trader, "path", 1, lambda: "client1", None, None, manager)

        bridge.register_callback_bridge("binding1", dispatcher)
        adapter = trader.register_callback.call_args[0][0]
        method = getattr(adapter, method_name)

        assert method(*payload) == "ok"
        dispatcher.assert_called_once_with("binding1", method_name, *payload)

    def test_invoke_async_bridge_wraps_callback(self):
        trader = MagicMock()
        trader.query_stock_positions_async.return_value = "started"
        dispatcher = Mock(return_value="done")
        manager = CallbackManager()
        bridge = TraderBridge(trader, "path", 1, lambda: "client1", None, None, manager)

        result = bridge.invoke_async_bridge(
            "query_stock_positions_async",
            ("account",),
            {},
            "cb1",
            dispatcher,
        )
        assert result == "started"

        callback = trader.query_stock_positions_async.call_args.kwargs["callback"]
        assert callback({"positions": []}) == "done"
        dispatcher.assert_called_once_with("cb1", {"positions": []})


class TestXtQuantServiceBridge:
    def test_subscribe_and_unsubscribe_bridge(self, mock_service):
        mock_service.exposed_authenticate("standard-user", "standard-secret")
        mock_service._xtdata = MagicMock()
        mock_service._xtdata.subscribe_quote.return_value = 100
        mock_service._xtdata.unsubscribe_quote.return_value = True
        dispatcher = Mock(return_value="ok")

        seq = mock_service.exposed_subscribe_xtdata_bridge(
            "subscribe_quote",
            ("000001.SZ",),
            {"period": "tick"},
            "cb1",
            dispatcher,
        )

        assert seq == 100
        callback = mock_service._xtdata.subscribe_quote.call_args.kwargs["callback"]
        assert callback({"000001.SZ": [{"lastPrice": 10.5}]}) == "ok"
        dispatcher.assert_called_once_with("cb1", {"000001.SZ": [{"lastPrice": 10.5}]})

        result = mock_service.exposed_unsubscribe_xtdata_bridge(100)
        assert result is True
        mock_service._xtdata.unsubscribe_quote.assert_called_once_with(100)

    def test_subscribe_bridge_permission_denied(self, mock_service):
        mock_service.exposed_authenticate("plus-user", "plus-secret")
        mock_service._xtdata = MagicMock()

        with pytest.raises(Exception) as exc_info:
            mock_service.exposed_subscribe_xtdata_bridge(
                "subscribe_quote",
                ("000001.SZ",),
                {"period": "tick"},
                "cb1",
                Mock(),
            )

        assert "callback" in str(exc_info.value)

    def test_formula_subscription_uses_formula_unsubscribe(self, mock_service):
        mock_service.exposed_authenticate("standard-user", "standard-secret")
        mock_service._xtdata = MagicMock()
        mock_service._xtdata.subscribe_formula.return_value = 900
        mock_service._xtdata.unsubscribe_formula.return_value = True

        seq = mock_service.exposed_subscribe_xtdata_bridge(
            "subscribe_formula",
            ("my_formula", "000001.SZ", "1d"),
            {},
            "cb_formula",
            Mock(return_value="ok"),
        )

        assert seq == 900
        result = mock_service.exposed_unsubscribe_xtdata_bridge(900)
        assert result is True
        mock_service._xtdata.unsubscribe_formula.assert_called_once_with(900)

    def test_unsubscribe_bridge_rejects_foreign_subscription(self, mock_service):
        mock_service.exposed_authenticate("standard-user", "standard-secret")
        mock_service._xtdata = MagicMock()
        XtQuantService._xtdata_subscriptions[123] = {
            "client_info": "other-client@127.0.0.1:9999",
            "callback_id": "cb_other",
            "method_name": "subscribe_quote",
            "unsubscribe_method": "unsubscribe_quote",
        }

        with pytest.raises(AuthError):
            mock_service.exposed_unsubscribe_xtdata_bridge(123)

        mock_service._xtdata.unsubscribe_quote.assert_not_called()
        assert 123 in XtQuantService._xtdata_subscriptions
        XtQuantService._xtdata_subscriptions.pop(123, None)


class TestSSLContext:
    def test_create_ssl_context_no_files(self):
        assert create_ssl_context(None, None) is None

    @patch('xqshare.server.ssl.SSLContext')
    def test_create_ssl_context_with_files(self, mock_ssl_context):
        mock_ctx = Mock()
        mock_ssl_context.return_value = mock_ctx
        with patch.object(mock_ctx, 'load_cert_chain'):
            create_ssl_context("cert.pem", "key.pem")
        mock_ssl_context.assert_called_once()

"""本地数据行情后端测试。

用构造的 DAT、LevelDB（DividData）和导出 JSON 验证 LocalXtdata 的读取、复权、
除权来源优先级与更新状态判断；真实数据的一致性另在大QMT 导出样本上验证。
"""

import datetime as dt
import json
import os
import shutil
import struct

import numpy as np
import pandas as pd
import pytest

from xqshare import local_xtdata
from xqshare.local_xtdata import (
    DAT_DTYPE,
    LocalXtdata,
    LocalXtdataConfig,
    parse_dividdata,
    read_leveldb,
)

BEIJING = dt.timezone(dt.timedelta(hours=8))


# ==================== 构造测试数据 ====================

def _day_seconds(date_text: str) -> int:
    """YYYYMMDD -> 北京时间 0 点的 Unix 秒（DAT 中 bar 时间的口径）。"""
    return int(dt.datetime.strptime(date_text, "%Y%m%d").replace(tzinfo=BEIJING).timestamp())


def _day_ms(date_text: str) -> int:
    return _day_seconds(date_text) * 1000


def write_dat(datadir, code: str, bars: list, dr: dict = None, tail: bytes = b"") -> None:
    """写日线 DAT：bars 为 [(日期, 开, 高, 低, 收, 量, 额, 昨收)]，dr 为 {日期: 当日除权系数}。"""
    number, market = code.split(".")
    folder = os.path.join(str(datadir), market, "86400")
    os.makedirs(folder, exist_ok=True)
    records = np.zeros(len(bars), dtype=DAT_DTYPE)
    cumulative = 1.0
    for i, (date, o, h, low, c, vol, amt, pre) in enumerate(bars):
        day_dr = (dr or {}).get(date, 1.0)
        cumulative *= day_dr
        records[i] = (_day_seconds(date), round(o * 1000), round(h * 1000), round(low * 1000), round(c * 1000),
                      vol, amt, day_dr, cumulative, round(pre * 1000))
    with open(os.path.join(folder, f"{number}.DAT"), "wb") as fh:
        fh.write(b"\xfe\xff\xff\xff\xff\xff\xff\x7f" + records.tobytes() + tail)


def _varint(value: int) -> bytes:
    out = bytearray()
    while value >= 0x80:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def _lp(data: bytes) -> bytes:
    return _varint(len(data)) + data


def _block(entries: list) -> bytes:
    """不做前缀压缩的 table 块 + 块尾（类型 0 未压缩，校验和不校验写 0）。"""
    body = b"".join(_varint(0) + _varint(len(k)) + _varint(len(v)) + k + v for k, v in entries)
    return body + struct.pack("<II", 0, 1)


def _log_record(payload: bytes) -> bytes:
    return struct.pack("<IHB", 0, len(payload), 1) + payload


def write_leveldb(folder, table_items: list, wal_items: list = ()) -> None:
    """写一个最小 LevelDB：一个 table 文件 + MANIFEST + CURRENT + 可选预写日志。

    table_items: [(key, 序号, value)]；wal_items: [(key, value 或 None 表示删除)]，序号接在 table 之后。
    """
    os.makedirs(str(folder), exist_ok=True)
    internal = sorted((k + struct.pack("<Q", (seq << 8) | 1), v) for k, seq, v in table_items)
    data = _block(internal)
    meta = _block([])
    data_handle = _varint(0) + _varint(len(data))
    index = _block([(internal[-1][0], data_handle)])
    buf = data + b"\x00\x00\x00\x00\x00"
    meta_off = len(buf)
    buf += meta + b"\x00" * 5
    index_off = len(buf)
    buf += index + b"\x00" * 5
    footer = _varint(meta_off) + _varint(len(meta)) + _varint(index_off) + _varint(len(index))
    buf += footer.ljust(40, b"\x00") + struct.pack("<Q", 0xDB4775248B80FB57)
    with open(os.path.join(str(folder), "000005.ldb"), "wb") as fh:
        fh.write(buf)

    edit = (_varint(1) + _lp(b"leveldb.BytewiseComparator") + _varint(2) + _varint(6) + _varint(3) + _varint(7)
            + _varint(4) + _varint(100) + _varint(7) + _varint(0) + _varint(5) + _varint(len(buf))
            + _lp(internal[0][0]) + _lp(internal[-1][0]))
    with open(os.path.join(str(folder), "MANIFEST-000004"), "wb") as fh:
        fh.write(_log_record(edit))
    with open(os.path.join(str(folder), "CURRENT"), "w") as fh:
        fh.write("MANIFEST-000004\n")

    batch = struct.pack("<QI", 50, len(wal_items))
    for key, value in wal_items:
        batch += (b"\x01" + _lp(key) + _lp(value)) if value is not None else (b"\x00" + _lp(key))
    with open(os.path.join(str(folder), "000006.log"), "wb") as fh:
        fh.write(_log_record(batch) if wal_items else b"")


def divid_value(ms: int, interest: float, bonus: float, gift: float, dr: float) -> bytes:
    """按 DividData 的 96 字节布局编码一条除权记录。"""
    value = bytearray(96)
    struct.pack_into("<q", value, 8, ms)
    struct.pack_into("<5d", value, 16, interest, bonus, gift, 0.0, 0.0)
    struct.pack_into("<i", value, 64, 0)
    struct.pack_into("<d", value, 72, dr)
    return bytes(value)


def write_json(export_dir, rel_path: str, obj) -> None:
    path = os.path.join(str(export_dir), rel_path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, ensure_ascii=False)


BARS = [
    ("20260105", 10.0, 10.5, 9.8, 10.2, 1000, 10_200_000, 10.0),
    ("20260106", 10.2, 10.4, 10.0, 10.3, 1100, 11_330_000, 10.2),
    # 20260107 停牌，除权日落在停牌日，应顺延到 20260108
    ("20260108", 9.3, 9.5, 9.2, 9.4, 1200, 11_280_000, 9.3),
    ("20260109", 9.4, 9.6, 9.3, 9.5, 1300, 12_350_000, 9.4),
]


@pytest.fixture
def env(tmp_path, monkeypatch):
    """构造 datadir + 导出目录；关闭重新加载间隔与 DividData 静默期，便于测试文件变化。"""
    monkeypatch.setattr(local_xtdata, "RELOAD_CHECK_SECONDS", 0)
    monkeypatch.setattr(local_xtdata, "DIVIDDATA_QUIET_SECONDS", 0)
    datadir, export = tmp_path / "datadir", tmp_path / "export"
    write_dat(datadir, "600000.SH", BARS, dr={"20260108": 1.1})
    write_leveldb(datadir / "DividData", [
        (b"SH|600000|4000|" + str(_day_ms("20260107")).encode(), 1,
         divid_value(_day_ms("20260107"), 1.0, 0.0, 0.0, 1.1)),
        (b"SH|600000|4000|999999999999", 2, b""),
    ])
    write_json(export, "meta/divid_factors.json",
               {"600000.SH": [[_day_ms("20260107"), 1.0, 0.0, 0.0, 0.0, 0.0, 0, 1.1]]})
    write_json(export, "meta/sectors.json", {"沪深A股": ["600000.SH"], "沪深ETF": ["510300.SH"]})
    write_json(export, "meta/instruments.json", {"600000.SH": {"InstrumentName": "浦发银行"}})
    config = LocalXtdataConfig(datadir=str(datadir), export_dir=str(export), divid_cache_dir=str(tmp_path / "cache"))
    return datadir, export, config


# ==================== LevelDB 解析 ====================

class TestLevelDB:
    def test_wal_overrides_and_deletes(self, tmp_path):
        write_leveldb(tmp_path / "db", [(b"a", 1, b"old"), (b"b", 2, b"keep-or-delete"), (b"c", 3, b"c")],
                      wal_items=[(b"a", b"new"), (b"b", None)])
        assert read_leveldb(str(tmp_path / "db")) == {b"a": b"new", b"c": b"c"}

    def test_parse_dividdata_skips_marker(self, env):
        datadir, _, _ = env
        events = parse_dividdata(str(datadir / "DividData"))
        assert events == {"600000.SH": [(_day_ms("20260107"), 1.0, 0.0, 0.0, 0.0, 0.0, 0, 1.1)]}


# ==================== 日线 ====================

class TestMarketData:
    def test_none_fields_and_dtypes(self, env):
        _, _, config = env
        frame = LocalXtdata(config).get_market_data_ex([], ["600000.SH"])["600000.SH"]
        assert list(frame.index) == ["20260105", "20260106", "20260108", "20260109"]
        assert list(frame.columns) == local_xtdata.KLINE_FIELDS
        assert frame["time"].iloc[0] == _day_ms("20260105")
        assert frame["close"].tolist() == [10.2, 10.3, 9.4, 9.5]
        assert frame["volume"].dtype == np.int64 and frame["amount"].iloc[0] == 10_200_000.0

    def test_range_count_and_unknown_fields(self, env):
        _, _, config = env
        result = LocalXtdata(config).get_market_data_ex(
            ["close", "lastClose"], ["600000.SH", "000001.SZ"], start_time="20260106",
            end_time="20260109150000", count=2)
        assert list(result["600000.SH"].index) == ["20260108", "20260109"]
        assert list(result["600000.SH"].columns) == ["close"]
        assert result["000001.SZ"].empty and list(result["000001.SZ"].columns) == ["close"]

    def test_partial_trailing_record_ignored(self, env):
        datadir, _, config = env
        write_dat(datadir, "600001.SH", BARS[:2], tail=b"\x01" * 20)
        assert len(LocalXtdata(config).get_market_data_ex(["close"], ["600001.SH"])["600001.SH"]) == 2

    def test_front_ratio_shifts_event_to_next_bar(self, env):
        _, _, config = env
        frame = LocalXtdata(config).get_market_data_ex(["close", "preClose", "volume"], ["600000.SH"],
                                                       dividend_type="front_ratio")["600000.SH"]
        # 除权日 0107 停牌：系数落在 0108，之前的 bar 乘 1/1.1，之后不变
        np.testing.assert_allclose(frame["close"], [10.2 / 1.1, 10.3 / 1.1, 9.4, 9.5])
        np.testing.assert_allclose(frame["preClose"], [10.0 / 1.1, 10.2 / 1.1, 9.3, 9.4])
        assert frame["volume"].tolist() == [1000, 1100, 1200, 1300]

    def test_front_ratio_anchor_is_latest_bar_even_with_range(self, env):
        _, _, config = env
        frame = LocalXtdata(config).get_market_data_ex(["close"], ["600000.SH"], end_time="20260106",
                                                       dividend_type="front_ratio")["600000.SH"]
        np.testing.assert_allclose(frame["close"], [10.2 / 1.1, 10.3 / 1.1])

    def test_unsupported_arguments(self, env):
        _, _, config = env
        x = LocalXtdata(config)
        with pytest.raises(ValueError):
            x.get_market_data_ex(["close"], ["600000.SH"], period="1m")
        with pytest.raises(ValueError):
            x.get_market_data_ex(["close"], ["600000.SH"], dividend_type="front")
        with pytest.raises(AttributeError, match="本地行情模式不支持 xtdata.subscribe_quote"):
            x.subscribe_quote


# ==================== 除权来源 ====================

class TestDividSources:
    def _set_export_dr(self, export, dr: float) -> None:
        write_json(export, "meta/divid_factors.json",
                   {"600000.SH": [[_day_ms("20260107"), 1.0, 0.0, 0.0, 0.0, 0.0, 0, dr]]})

    def test_export_is_primary_by_default(self, env):
        _, export, config = env
        self._set_export_dr(export, 1.2)
        assert LocalXtdata(config).get_divid_factors("600000.SH")["dr"].tolist() == [1.2]

    def test_dividdata_primary_when_configured(self, env):
        _, export, config = env
        self._set_export_dr(export, 1.2)
        config.divid_source = local_xtdata.DIVID_SOURCE_DIVIDDATA
        assert LocalXtdata(config).get_divid_factors("600000.SH")["dr"].tolist() == [1.1]

    def test_fallback_to_other_source_then_dat(self, env):
        datadir, export, config = env
        write_json(export, "meta/divid_factors.json", {})
        assert LocalXtdata(config).get_divid_factors("600000.SH")["dr"].tolist() == [1.1]
        # 两个来源都没有：用 DAT 自带的单日 dr，日期为实际生效的 bar
        write_dat(datadir, "600002.SH", BARS, dr={"20260109": 1.05})
        frame = LocalXtdata(config).get_divid_factors("600002.SH")
        assert list(frame.index) == ["20260109"]
        np.testing.assert_allclose(frame["dr"], [1.05], rtol=1e-6)

    def test_divid_frame_format_and_range(self, env):
        _, _, config = env
        frame = LocalXtdata(config).get_divid_factors("600000.SH")
        assert list(frame.columns) == local_xtdata.DIVID_FIELDS
        assert list(frame.index) == ["20260107"] and frame["time"].iloc[0] == _day_ms("20260107")
        assert LocalXtdata(config).get_divid_factors("600000.SH", start_time="20260108").empty

    def test_dividdata_copied_to_cache_and_reloaded(self, env):
        datadir, _, config = env
        config.divid_source = local_xtdata.DIVID_SOURCE_DIVIDDATA
        x = LocalXtdata(config)
        assert x.get_divid_factors("600000.SH")["dr"].tolist() == [1.1]
        assert os.path.exists(os.path.join(config.divid_cache_dir, "DividData", "CURRENT"))
        write_leveldb(datadir / "DividData", [
            (b"SH|600000|4000|" + str(_day_ms("20260107")).encode(), 1,
             divid_value(_day_ms("20260107"), 1.0, 0.0, 0.0, 1.3)),
        ])
        os.utime(datadir / "DividData" / "000005.ldb", (1, 1))  # 保证签名变化且早于静默期
        assert x.get_divid_factors("600000.SH")["dr"].tolist() == [1.3]

    def test_dividdata_failure_keeps_previous(self, env):
        datadir, _, config = env
        config.divid_source = local_xtdata.DIVID_SOURCE_DIVIDDATA
        x = LocalXtdata(config)
        assert x.get_divid_factors("600000.SH")["dr"].tolist() == [1.1]
        with open(datadir / "DividData" / "CURRENT", "w") as fh:
            fh.write("MANIFEST-999999\n")
        assert x.get_divid_factors("600000.SH")["dr"].tolist() == [1.1]


# ==================== 板块、品种、快照、状态 ====================

class TestMetaAndStatus:
    def test_sectors_and_instruments(self, env):
        _, _, config = env
        x = LocalXtdata(config)
        assert x.get_sector_list() == ["沪深A股", "沪深ETF"]
        assert x.get_stock_list_in_sector("沪深A股") == ["600000.SH"]
        assert x.get_stock_list_in_sector("不存在") == []
        assert x.get_instrument_detail("600000.SH") == {"InstrumentName": "浦发银行"}
        assert x.get_instrument_detail_list(["600000.SH", "X"]) == {
            "600000.SH": {"InstrumentName": "浦发银行"}, "X": None}

    def test_full_tick_only_for_today(self, env):
        datadir, _, config = env
        x = LocalXtdata(config)
        assert x.get_full_tick(["SH"]) == {}
        today = dt.datetime.now(BEIJING).strftime("%Y%m%d")
        write_dat(datadir, "600000.SH", BARS + [(today, 9.5, 9.9, 9.4, 9.8, 1400, 13_720_000, 9.5)])
        tick = x.get_full_tick(["SH"])["600000.SH"]
        assert tick["lastPrice"] == 9.8 and tick["lastClose"] == 9.5 and tick["volume"] == 1400
        assert "askPrice" not in tick

    def test_download_is_noop_with_callback(self, env):
        _, _, config = env
        received = []
        assert LocalXtdata(config).download_history_data2(["a", "b"], "1d", callback=received.append) is None
        assert received[0]["finished"] == received[0]["total"] == 2

    def test_update_status(self, env):
        _, export, config = env
        x = LocalXtdata(config)
        assert "未找到 ready.json" in x.describe_update_status()
        write_json(export, "status.json", {"state": "running", "started_at": "t"})
        assert "正在更新" in x.describe_update_status()
        write_json(export, "status.json", {"state": "done"})
        write_json(export, "ready.json", {"trade_date": "20200101", "finished_at": "t"})
        assert "可能未更新" in x.describe_update_status()
        write_json(export, "ready.json", {"trade_date": "29991231", "finished_at": "t"})
        assert x.describe_update_status().startswith("数据已就绪")

    def test_expected_trade_date(self):
        friday_evening = dt.datetime(2026, 11, 20, 17, 0, tzinfo=BEIJING)
        saturday = dt.datetime(2026, 11, 21, 10, 0, tzinfo=BEIJING)
        monday_morning = dt.datetime(2026, 11, 23, 9, 0, tzinfo=BEIJING)
        assert local_xtdata._expected_trade_date(friday_evening) == "20261120"
        assert local_xtdata._expected_trade_date(saturday) == "20261120"
        assert local_xtdata._expected_trade_date(monday_morning) == "20261120"

    def test_self_check(self, env):
        _, export, config = env
        x = LocalXtdata(config)
        expected = x.get_market_data_ex(local_xtdata.PRICE_FIELDS, ["600000.SH"], dividend_type="front_ratio")
        folder = os.path.join(str(export), "validation", "front_ratio")
        os.makedirs(folder)
        expected["600000.SH"].rename_axis("date").to_csv(os.path.join(folder, "600000.SH.csv"))
        assert x.run_self_check() == {"600000.SH": 0.0}


# ==================== 审查问题回归 ====================

class TestReviewRegressions:
    def test_multiple_wal_logs_replayed_in_order(self, tmp_path):
        write_leveldb(tmp_path / "db", [(b"a", 1, b"table")], wal_items=[(b"a", b"wal6")])
        batch = struct.pack("<QI", 60, 1) + b"\x01" + _lp(b"a") + _lp(b"wal7")
        with open(tmp_path / "db" / "000007.log", "wb") as fh:
            fh.write(_log_record(batch))
        with open(tmp_path / "db" / "000003.log", "wb") as fh:  # 早于 log_number 的旧日志应忽略
            fh.write(_log_record(struct.pack("<QI", 99, 1) + b"\x01" + _lp(b"a") + _lp(b"stale")))
        assert read_leveldb(str(tmp_path / "db")) == {b"a": b"wal7"}

    def test_dat_shorter_than_header(self, env):
        datadir, _, config = env
        with open(datadir / "SH" / "86400" / "600003.DAT", "wb") as fh:
            fh.write(b"\xfe\xff")
        result = LocalXtdata(config).get_market_data_ex(["close"], ["600003.SH", "600000.SH"])
        assert result["600003.SH"].empty and len(result["600000.SH"]) == 4

    @pytest.mark.parametrize("start, end", [
        (20260106, 20260108),
        ("20260106093000", "20260108150000"),
        (dt.date(2026, 1, 6), dt.datetime(2026, 1, 8, 15)),
        (_day_ms("20260106"), str(_day_ms("20260108"))),
    ])
    def test_date_argument_formats(self, env, start, end):
        _, _, config = env
        frame = LocalXtdata(config).get_market_data_ex(["close"], ["600000.SH"], start_time=start,
                                                       end_time=end)["600000.SH"]
        assert list(frame.index) == ["20260106", "20260108"]

    def test_unrecognized_date_raises(self, env):
        _, _, config = env
        with pytest.raises(ValueError, match="无法识别的时间参数"):
            LocalXtdata(config).get_market_data_ex(["close"], ["600000.SH"], start_time="2026-01-06")

    def test_empty_divid_with_range(self, env):
        datadir, _, config = env
        write_dat(datadir, "000001.SH", BARS)
        frame = LocalXtdata(config).get_divid_factors("000001.SH", start_time="20200101")
        assert frame.empty and list(frame.columns) == local_xtdata.DIVID_FIELDS

    def test_missing_sources_are_throttled(self, env, monkeypatch, caplog):
        datadir, export, config = env
        monkeypatch.setattr(local_xtdata, "RELOAD_CHECK_SECONDS", 60)
        shutil.rmtree(datadir / "DividData")
        os.remove(export / "meta" / "divid_factors.json")
        x = LocalXtdata(config)
        calls = []

        def missing_signature(folder):
            calls.append(folder)
            raise OSError("missing")

        monkeypatch.setattr(local_xtdata, "_dir_signature", missing_signature)
        with caplog.at_level("WARNING", logger="xqshare.local_xtdata"):
            for _ in range(5):
                x.get_market_data_ex(["close"], ["600000.SH"], dividend_type="front_ratio")
        assert len(calls) == 1
        assert sum("divid_factors.json" in r.message for r in caplog.records) == 1

    def test_self_check_errors_do_not_raise(self, env):
        _, export, config = env
        folder = os.path.join(str(export), "validation", "front_ratio")
        os.makedirs(folder)
        pd.DataFrame({"date": ["20260105"], "close": [1.0]}).to_csv(os.path.join(folder, "600000.SH.csv"), index=False)
        assert LocalXtdata(config).run_self_check() == {"600000.SH": float("inf")}

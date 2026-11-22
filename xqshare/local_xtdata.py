"""本地数据行情后端（替代 xtquant.xtdata 的只读实现）。

所属层次：
    xqshare 服务端的数据源适配层。miniQMT 关闭后外部 xtdata 无法连接，本模块直接读取
    大QMT 本机文件，向客户端提供与 xtdata 同名、同参数、同返回格式的行情接口。

数据来源：
    1. 日线 K 线：<datadir>/<SH|SZ>/86400/<6位代码>.DAT，由大QMT 内置策略每日 download 更新；
    2. 除权数据，三个来源按优先级合并（XQSHARE_DIVID_SOURCE 决定前两者谁为准）：
       - export：内置策略导出的 <export>/meta/divid_factors.json（即 API 返回值，含盘中修正）；
       - dividdata：<datadir>/DividData（LevelDB，客户端启动时更新），定期复制到本地缓存目录后解析；
       - DAT 兜底：K 线记录中自带的单日除权系数，只有 dr 没有明细；
    3. 板块成份、品种信息：<export>/meta/sectors.json、instruments.json；
       股本表：<datadir>/Finance/<SH|SZ>/86400/<代码>_7004.DAT；
    4. 更新状态：<export>/status.json、ready.json，用于判断数据是否已更新到最新交易日。

支持的接口：
    get_market_data_ex（仅 1d，dividend_type 支持 none / front_ratio）、get_divid_factors、
    get_stock_list_in_sector、get_sector_list、get_instrument_detail、get_instrument_detail_list、
    get_financial_data（仅 Capital 股本表）、get_full_tick（用当日已收盘日线近似）、
    download_history_data / download_history_data2（空操作）。
    另有 get_update_status、get_adjust_samples（非 xtdata 接口），供服务端返回内置策略的更新状态和复权样本。

前复权算法（已与 QMT 的 front_ratio 结果逐 bar 对照一致）：
    每个除权事件映射到不早于除权日的第一根 bar（停牌期间的除权顺延到复牌日），
    该 bar 的系数乘上 dr，累乘得到累计因子 cum，复权价 = 原价 × cum / cum[-1]。
    open/high/low/close/preClose 参与复权，volume/amount 不变。
"""

import datetime as dt
import json
import logging
import os
import shutil
import struct
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger("xqshare.local_xtdata")

# ==================== 配置 ====================

DATADIR_ENV = "XQSHARE_QMT_DATADIR"
EXPORT_DIR_ENV = "XQSHARE_QMT_EXPORT_DIR"
DIVID_SOURCE_ENV = "XQSHARE_DIVID_SOURCE"
DIVID_CACHE_DIR_ENV = "XQSHARE_DIVID_CACHE_DIR"

DIVID_SOURCE_EXPORT = "export"
DIVID_SOURCE_DIVIDDATA = "dividdata"

# 文件变化检查间隔（秒）：避免每次请求都 stat 文件
RELOAD_CHECK_SECONDS = 60
# DividData 源目录最近修改不足该秒数时不复制，避开大QMT 启动时的写入窗口
DIVIDDATA_QUIET_SECONDS = 60
# 收盘后预期数据已更新的时间点，用于判断数据是否落后
DATA_READY_TIME = dt.time(16, 30)

# get_divid_factors 返回列（按 xtdata 示例输出：time 为除权日毫秒时间戳，index 为 YYYYMMDD）
DIVID_FIELDS = ["time", "interest", "stockBonus", "stockGift", "allotNum", "allotPrice", "gugai", "dr"]
# 本地日线支持的字段；front_ratio 时只有价格字段参与复权
KLINE_FIELDS = ["time", "open", "high", "low", "close", "volume", "amount", "preClose"]
PRICE_FIELDS = ["open", "high", "low", "close", "preClose"]
# 内置策略导出复权样本（validation/<复权类型>/<代码>.csv）的复权类型
ADJUST_SAMPLE_TYPES = ("front_ratio", "back_ratio", "front", "back")

# 日线 DAT：8 字节文件头 + 每根 bar 64 字节；价格为 1/1000 元整数，time 为北京时间 0 点的 Unix 秒
DAT_HEADER_SIZE = 8
DAT_DTYPE = np.dtype({
    "names": ["time", "open", "high", "low", "close", "volume", "amount", "dr", "cum_dr", "preClose"],
    "formats": ["<u4", "<u4", "<u4", "<u4", "<u4", "<u4", "<u8", "<f4", "<f4", "<u4"],
    "offsets": [0, 4, 8, 12, 16, 24, 32, 44, 48, 52],
    "itemsize": 64,
})
BEIJING_OFFSET_SECONDS = 8 * 3600

# 股本表 DAT：无文件头，每期 56 字节（已与内置 xtdata.get_financial_data 逐字段对照一致）；
# 两个日期为北京时间 0 点的毫秒时间戳，股本单位为股
CAPITAL_DTYPE = np.dtype([
    ("m_timetag", "<i8"), ("m_anntime", "<i8"), ("total_capital", "<f8"), ("circulating_capital", "<f8"),
    ("restrict_circulating_capital", "<f8"), ("freeFloatCapital", "<f8"), ("m_quarter", "<i8"),
])
CAPITAL_COLUMNS = list(CAPITAL_DTYPE.names)

# 单个除权事件：(毫秒时间, 每股红利, 送股, 转增, 配股, 配股价, 股改标志, dr)
DividEvent = Tuple[int, float, float, float, float, float, int, float]


@dataclass
class LocalXtdataConfig:
    """本地数据后端配置。

    Attributes:
        datadir: 大QMT 的 datadir 目录，例如 E:\\swhyqmt\\datadir。
        export_dir: 内置策略的输出目录，例如 E:\\swhyqmt\\export。
        divid_source: 除权数据以哪个来源为准，export 或 dividdata。
        divid_cache_dir: DividData 复制到的本地缓存目录。
    """

    datadir: str
    export_dir: str
    divid_source: str = DIVID_SOURCE_EXPORT
    divid_cache_dir: str = "cache"

    @classmethod
    def from_env(cls) -> "LocalXtdataConfig":
        """从环境变量读取配置；必填项缺失或取值非法时直接报错，避免带病启动。"""
        datadir = os.environ.get(DATADIR_ENV, "").strip()
        export_dir = os.environ.get(EXPORT_DIR_ENV, "").strip()
        if not datadir or not export_dir:
            raise ValueError(f"本地行情模式需要设置 {DATADIR_ENV} 和 {EXPORT_DIR_ENV}")
        divid_source = os.environ.get(DIVID_SOURCE_ENV, DIVID_SOURCE_EXPORT).strip().lower()
        if divid_source not in (DIVID_SOURCE_EXPORT, DIVID_SOURCE_DIVIDDATA):
            raise ValueError(f"{DIVID_SOURCE_ENV} 只能是 export 或 dividdata，当前为 {divid_source!r}")
        cache_dir = os.environ.get(DIVID_CACHE_DIR_ENV, "cache").strip() or "cache"
        return cls(datadir=datadir, export_dir=export_dir, divid_source=divid_source, divid_cache_dir=cache_dir)


# ==================== LevelDB 只读解析 ====================
# DividData 的数据块均未压缩，按 LevelDB 文件格式直接解析，无需安装 plyvel。

_LOG_BLOCK_SIZE = 32768


def _varint(buf: bytes, pos: int) -> Tuple[int, int]:
    """解析 LevelDB varint，返回 (值, 新位置)。"""
    result = shift = 0
    while True:
        byte = buf[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        shift += 7
        if byte < 0x80:
            return result, pos


def _length_prefixed(buf: bytes, pos: int) -> Tuple[bytes, int]:
    """解析 varint 长度前缀的字节串。"""
    length, pos = _varint(buf, pos)
    return buf[pos:pos + length], pos + length


def _log_records(data: bytes) -> List[bytes]:
    """解析 LevelDB 日志格式（MANIFEST 与预写日志共用）：32KB 分块，单条记录可跨块分片。"""
    records: List[bytes] = []
    fragment = b""
    pos = 0
    while pos + 7 <= len(data):
        left = _LOG_BLOCK_SIZE - pos % _LOG_BLOCK_SIZE
        if left < 7:
            pos += left  # 块尾不足一个记录头的部分是填充
            continue
        length, record_type = struct.unpack_from("<HB", data, pos + 4)
        payload = data[pos + 7:pos + 7 + length]
        pos += 7 + length
        if record_type == 1:
            records.append(payload)
        elif record_type == 2:
            fragment = payload
        elif record_type == 3:
            fragment += payload
        elif record_type == 4:
            records.append(fragment + payload)
            fragment = b""
    return records


def _live_tables(db_dir: str) -> Tuple[List[int], int, int]:
    """读 CURRENT 指向的 MANIFEST，回放版本变更，得到有效的 table 文件编号、当前与前一个预写日志编号。"""
    with open(os.path.join(db_dir, "CURRENT"), "rb") as fh:
        manifest_name = fh.read().decode().strip()
    with open(os.path.join(db_dir, manifest_name), "rb") as fh:
        manifest = fh.read()
    tables: set = set()
    log_number = prev_log_number = 0
    for record in _log_records(manifest):
        pos = 0
        while pos < len(record):
            tag, pos = _varint(record, pos)
            if tag == 1:  # comparator 名称
                _, pos = _length_prefixed(record, pos)
            elif tag in (2, 3, 4, 9):  # 日志编号 / 下一个文件编号 / 最大序号 / 前一日志编号
                value, pos = _varint(record, pos)
                if tag == 2:
                    log_number = value
                elif tag == 9:
                    prev_log_number = value
            elif tag == 5:  # compact pointer
                _, pos = _varint(record, pos)
                _, pos = _length_prefixed(record, pos)
            elif tag == 6:  # 删除文件
                _, pos = _varint(record, pos)
                number, pos = _varint(record, pos)
                tables.discard(number)
            elif tag == 7:  # 新增文件
                _, pos = _varint(record, pos)
                number, pos = _varint(record, pos)
                _, pos = _varint(record, pos)
                _, pos = _length_prefixed(record, pos)
                _, pos = _length_prefixed(record, pos)
                tables.add(number)
            else:
                raise ValueError(f"MANIFEST 中出现未知字段 tag={tag}")
    return sorted(tables), log_number, prev_log_number


def _block_entries(block: bytes):
    """遍历一个 table 块中的 (key, value)，处理 key 的前缀压缩。"""
    restart_count = struct.unpack_from("<I", block, len(block) - 4)[0]
    end = len(block) - 4 - 4 * restart_count
    pos = 0
    last_key = b""
    while pos < end:
        shared, pos = _varint(block, pos)
        non_shared, pos = _varint(block, pos)
        value_length, pos = _varint(block, pos)
        key = last_key[:shared] + block[pos:pos + non_shared]
        pos += non_shared
        yield key, block[pos:pos + value_length]
        pos += value_length
        last_key = key


def _read_table_block(buf: bytes, handle: bytes) -> bytes:
    """按块句柄读取块内容；块尾类型字节非 0 表示压缩，本解析器不支持。"""
    offset, pos = _varint(handle, 0)
    size, _ = _varint(handle, pos)
    if buf[offset + size] != 0:
        raise ValueError("DividData 出现压缩数据块，当前解析器不支持")
    return buf[offset:offset + size]


def _table_entries(path: str):
    """遍历 table 文件，产出 (user_key, 序号, 类型, value)；类型 1 为写入，0 为删除。"""
    with open(path, "rb") as fh:
        buf = fh.read()
    footer = buf[-48:]
    _, pos = _varint(footer, 0)  # 跳过 metaindex 句柄
    _, pos = _varint(footer, pos)
    index_block = _read_table_block(buf, footer[pos:])
    for _, handle in _block_entries(index_block):
        for internal_key, value in _block_entries(_read_table_block(buf, handle)):
            tag = struct.unpack_from("<Q", internal_key, len(internal_key) - 8)[0]
            yield internal_key[:-8], tag >> 8, tag & 0xFF, value


def read_leveldb(db_dir: str) -> Dict[bytes, bytes]:
    """只读解析整个 LevelDB 目录，返回 {key: value}；同一 key 取最大序号，删除的 key 不返回。"""
    latest: Dict[bytes, Tuple[int, int, bytes]] = {}

    def put(key: bytes, sequence: int, value_type: int, value: bytes) -> None:
        current = latest.get(key)
        if current is None or sequence > current[0]:
            latest[key] = (sequence, value_type, value)

    tables, log_number, prev_log_number = _live_tables(db_dir)
    for number in tables:
        candidates = [os.path.join(db_dir, f"{number:06d}.{ext}") for ext in ("ldb", "sst")]
        path = next((p for p in candidates if os.path.exists(p)), None)
        if path is None:
            raise FileNotFoundError(f"DividData 缺少 table 文件 {number:06d}")
        for key, sequence, value_type, value in _table_entries(path):
            put(key, sequence, value_type, value)

    # 与 LevelDB 自身恢复逻辑一致：回放编号不小于 log_number 的全部预写日志（以及 prev_log_number），
    # memtable 正在落盘时可能同时存在新旧两个日志，只读一个会漏掉最新写入
    wal_numbers = sorted(int(name[:-4]) for name in os.listdir(db_dir)
                         if name.endswith(".log") and name[:-4].isdigit()
                         and (int(name[:-4]) >= log_number or int(name[:-4]) == prev_log_number))
    for wal_number in wal_numbers:
        with open(os.path.join(db_dir, f"{wal_number:06d}.log"), "rb") as fh:
            wal = fh.read()
        for record in _log_records(wal):
            sequence, count = struct.unpack_from("<QI", record, 0)
            pos = 12
            for offset in range(count):
                value_type = record[pos]
                pos += 1
                key, pos = _length_prefixed(record, pos)
                value = b""
                if value_type == 1:
                    value, pos = _length_prefixed(record, pos)
                put(key, sequence + offset, value_type, value)

    return {key: value for key, (_, value_type, value) in latest.items() if value_type == 1}


def parse_dividdata(db_dir: str) -> Dict[str, List[DividEvent]]:
    """解析 DividData，返回沪深品种的除权事件 {代码: [事件, ...]}（按时间升序）。

    key 格式为 "市场|代码|4000|毫秒时间"，value 为 96 字节：
    偏移 8 毫秒时间，16~48 五个 double（红利/送股/转增/配股/配股价），64 int32 股改标志，72 double dr。
    每个品种另有一条时间为 999999999999、value 为空的占位记录，跳过。
    """
    events: Dict[str, List[DividEvent]] = {}
    for key, value in read_leveldb(db_dir).items():
        parts = key.split(b"|")
        if len(parts) != 4 or parts[2] != b"4000" or parts[0] not in (b"SH", b"SZ") or len(value) < 80:
            continue
        code = f"{parts[1].decode()}.{parts[0].decode()}"
        ms = struct.unpack_from("<q", value, 8)[0]
        interest, bonus, gift, allot_num, allot_price = struct.unpack_from("<5d", value, 16)
        gugai = struct.unpack_from("<i", value, 64)[0]
        dr = struct.unpack_from("<d", value, 72)[0]
        events.setdefault(code, []).append((ms, interest, bonus, gift, allot_num, allot_price, gugai, dr))
    for code_events in events.values():
        code_events.sort()
    return events


# ==================== DAT 日线读取 ====================

def read_daily_dat(path: str) -> np.ndarray:
    """读取日线 DAT 的全部完整记录；文件末尾不足 64 字节的部分（正在写入）忽略。"""
    with open(path, "rb") as fh:
        buf = fh.read()
    if len(buf) < DAT_HEADER_SIZE:  # 文件刚被重建、连文件头都还没写完
        return np.zeros(0, dtype=DAT_DTYPE)
    count = (len(buf) - DAT_HEADER_SIZE) // DAT_DTYPE.itemsize
    return np.frombuffer(buf, DAT_DTYPE, count=count, offset=DAT_HEADER_SIZE)


def _bar_dates(seconds: np.ndarray) -> List[str]:
    """bar 时间（Unix 秒）转北京时间日期字符串 YYYYMMDD。"""
    days = ((seconds.astype("int64") + BEIJING_OFFSET_SECONDS) // 86400).astype("datetime64[D]")
    return [text.replace("-", "") for text in np.datetime_as_string(days, unit="D")]


def _normalize_date(value: Any) -> str:
    """把 xtdata 支持的时间参数统一成 YYYYMMDD。

    支持：空、YYYYMMDD / YYYYMMDDhhmmss（字符串或整数）、毫秒时间戳（13 位）、datetime / date。
    """
    if value is None or value == "":
        return ""
    if isinstance(value, (dt.datetime, dt.date)):
        return value.strftime("%Y%m%d")
    text = str(int(value)) if isinstance(value, (int, float, np.integer, np.floating)) else str(value).strip()
    if len(text) == 13 and text.isdigit():  # 毫秒时间戳
        seconds = int(text) // 1000 + BEIJING_OFFSET_SECONDS
        return dt.datetime.fromtimestamp(seconds, dt.timezone.utc).strftime("%Y%m%d")
    if len(text) in (8, 14) and text.isdigit():
        return text[:8]
    raise ValueError(f"无法识别的时间参数: {value!r}，应为 YYYYMMDD / YYYYMMDDhhmmss / 毫秒时间戳")


# ==================== 本地 xtdata ====================

class LocalXtdata:
    """本地文件版 xtdata。

    作为 XtQuantService._xtdata 注入服务端，客户端调用方式与真实 xtdata 完全一致。
    日线每次请求直接读 DAT（单文件约 1 毫秒，不做缓存，天然读到最新数据）；
    除权、板块、品种信息在内存中缓存，文件变化后自动重新加载。

    Attributes:
        config: 后端配置。
    """

    def __init__(self, config: LocalXtdataConfig):
        self.config = config
        self._lock = threading.RLock()
        self._json_cache: Dict[str, Tuple[float, Any]] = {}
        self._json_checked: Dict[str, float] = {}
        self._export_events: Tuple[Any, Optional[Dict[str, List[DividEvent]]]] = (None, None)
        self._dividdata_events: Optional[Dict[str, List[DividEvent]]] = None
        self._dividdata_signature: Optional[tuple] = None
        self._dividdata_checked = 0.0
        self._compared_versions: Optional[tuple] = None
        self._status_checked = 0.0
        self._last_status_message = ""

    @classmethod
    def from_env(cls) -> "LocalXtdata":
        """按环境变量创建实例。"""
        return cls(LocalXtdataConfig.from_env())

    def __getattr__(self, name: str) -> Any:
        """未实现的 xtdata 接口给出明确提示，而不是含糊的属性错误。"""
        if name.startswith("_"):
            raise AttributeError(name)
        raise AttributeError(f"本地行情模式不支持 xtdata.{name}")

    # ---------- 行情 ----------

    def get_market_data_ex(self, field_list: Sequence[str] = (), stock_list: Sequence[str] = (),
                           period: str = "1d", start_time: Any = "", end_time: Any = "", count: int = -1,
                           dividend_type: str = "none", fill_data: bool = True) -> Dict[str, pd.DataFrame]:
        """读取日线，返回 {代码: DataFrame}，index 为 YYYYMMDD 字符串，columns 为请求的字段。

        Args:
            field_list: 字段列表，为空时返回全部支持字段；不支持的字段忽略（与 xtdata 行为一致）。
            stock_list: 代码列表；没有数据的代码返回空 DataFrame。
            period: 只支持 1d。
            start_time / end_time: 闭区间日期过滤。
            count: 大于 0 时取区间内最后 count 根。
            dividend_type: none 或 front_ratio；前复权以全部本地数据的最新 bar 为基准，再做区间截取。
            fill_data: 本地数据不做填充，参数保留以兼容调用方。
        """
        if period != "1d":
            raise ValueError(f"本地行情模式只支持 period='1d'，收到 {period!r}")
        if dividend_type not in ("none", "front_ratio"):
            raise ValueError(f"本地行情模式只支持 dividend_type 为 none / front_ratio，收到 {dividend_type!r}")
        self._check_update_status()
        fields = [f for f in field_list if f in KLINE_FIELDS] if field_list else list(KLINE_FIELDS)
        start, end = _normalize_date(start_time), _normalize_date(end_time)
        return {code: self._daily_frame(code, fields, start, end, count, dividend_type) for code in stock_list}

    def _daily_frame(self, code: str, fields: List[str], start: str, end: str, count: int,
                     dividend_type: str) -> pd.DataFrame:
        """读取单个品种日线并按参数复权、过滤。"""
        bars = self._read_bars(code)
        if bars is None or len(bars) == 0:
            return pd.DataFrame(columns=fields)
        time_ms = bars["time"].astype("int64") * 1000
        data: Dict[str, np.ndarray] = {
            "time": time_ms,
            "volume": bars["volume"].astype("int64"),
            "amount": bars["amount"].astype("float64"),
        }
        for name in PRICE_FIELDS:
            data[name] = bars[name].astype("float64") / 1000
        if dividend_type == "front_ratio":
            factor = self._front_ratio_factor(code, time_ms, bars)
            for name in PRICE_FIELDS:
                data[name] = data[name] * factor
        frame = pd.DataFrame({name: data[name] for name in fields}, index=_bar_dates(bars["time"]))
        if start:
            frame = frame[frame.index >= start]
        if end:
            frame = frame[frame.index <= end]
        if count is not None and count > 0:
            frame = frame.iloc[-count:]
        return frame

    def _read_bars(self, code: str) -> Optional[np.ndarray]:
        """按代码定位并读取 DAT；代码格式不合法或文件不存在时返回 None。"""
        path = self._dat_path(code)
        if path is None or not os.path.exists(path):
            return None
        return read_daily_dat(path)

    def _dat_path(self, code: str) -> Optional[str]:
        """600519.SH -> <datadir>/SH/86400/600519.DAT。"""
        parts = str(code).split(".")
        if len(parts) != 2:
            return None
        return os.path.join(self.config.datadir, parts[1].upper(), "86400", f"{parts[0]}.DAT")

    def _front_ratio_factor(self, code: str, time_ms: np.ndarray, bars: np.ndarray) -> np.ndarray:
        """计算等比前复权系数：事件 dr 落到不早于除权日的第一根 bar，累乘后以最新 bar 归一。"""
        events = self._divid_events(code, lambda: bars)
        per_bar = np.ones(len(time_ms))
        for event in events:
            idx = int(np.searchsorted(time_ms, event[0], side="left"))
            if idx < len(per_bar):
                per_bar[idx] *= event[7]
        cumulative = np.cumprod(per_bar)
        return cumulative / cumulative[-1]

    def get_full_tick(self, code_list: Sequence[str]) -> Dict[str, Dict[str, Any]]:
        """用当日已收盘的日线近似全推快照。

        本地没有实时行情：只有最后一根日线日期等于今天（北京时间）的品种才返回，
        盘中或当日数据尚未更新时返回空，调用方会按"取不到快照"处理，避免把昨天的价格当成今天。
        不提供五档盘口字段，防止下单逻辑误用。传入 SH / SZ 市场代码时展开为该市场全部已知品种。
        """
        today = dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).strftime("%Y%m%d")
        codes: List[str] = []
        for item in code_list:
            if item in ("SH", "SZ"):
                codes.extend(code for code in self._instruments() if code.endswith("." + item))
            else:
                codes.append(item)
        result: Dict[str, Dict[str, Any]] = {}
        for code in codes:
            bars = self._read_bars(code)
            if bars is None or len(bars) == 0:
                continue
            last = bars[-1]
            if _bar_dates(bars["time"][-1:])[0] != today:
                continue
            result[code] = {
                "timetag": f"{today} 15:00:00",
                "lastPrice": last["close"] / 1000,
                "open": last["open"] / 1000,
                "high": last["high"] / 1000,
                "low": last["low"] / 1000,
                "lastClose": last["preClose"] / 1000,
                "amount": float(last["amount"]),
                "volume": int(last["volume"]),
                "pvolume": int(last["volume"]) * 100,
            }
        return result

    def download_history_data(self, stock_code: str, period: str = "1d", start_time: Any = "", end_time: Any = "",
                              incrementally: Optional[bool] = None) -> None:
        """空操作：本地模式下数据由大QMT 内置策略每日下载。"""
        return None

    def download_history_data2(self, stock_list: Sequence[str], period: str = "1d", start_time: Any = "",
                               end_time: Any = "", callback: Optional[Callable] = None,
                               incrementally: Optional[bool] = None) -> None:
        """空操作，并立即回调一次"已完成"，让等待下载进度的调用方正常结束。"""
        if callback is not None:
            total = len(stock_list)
            callback({"finished": total, "total": total, "stockcode": "",
                      "message": "本地行情模式：数据由大QMT 内置策略更新"})
        return None

    # ---------- 除权 ----------

    def get_divid_factors(self, stock_code: str, start_time: Any = "", end_time: Any = "") -> pd.DataFrame:
        """返回除权数据 DataFrame：index 为除权日 YYYYMMDD，列为 DIVID_FIELDS。"""
        # 只有两个除权来源都没有该品种时才需要读 DAT 兜底，按需读取，避免批量取除权时逐只解析日线
        events = self._divid_events(stock_code, lambda: self._read_bars(stock_code))
        seconds = np.array([event[0] // 1000 for event in events], dtype="int64")
        frame = pd.DataFrame([list(event) for event in events], columns=DIVID_FIELDS,
                             index=pd.Index(_bar_dates(seconds), dtype=object))
        start, end = _normalize_date(start_time), _normalize_date(end_time)
        if start:
            frame = frame[frame.index >= start]
        if end:
            frame = frame[frame.index <= end]
        return frame

    def _divid_events(self, code: str, load_bars: Callable[[], Optional[np.ndarray]]) -> List[DividEvent]:
        """按配置的优先级取某品种的除权事件：为准的来源 -> 另一来源 -> DAT 自带 dr。

        Args:
            code: 品种代码。
            load_bars: 返回该品种日线记录的函数，只在两个来源都没有该品种、需要 DAT 兜底时才调用。
        """
        export_events = self._export_divid()
        dividdata_events = self._dividdata()
        self._compare_divid_sources(export_events, dividdata_events)
        sources = [export_events, dividdata_events]
        if self.config.divid_source == DIVID_SOURCE_DIVIDDATA:
            sources.reverse()
        for source in sources:
            if source is not None and code in source:
                return source[code]
        bars = load_bars()
        if bars is None or len(bars) == 0:
            return []
        # 两个来源都没有该品种：用 K 线记录里的单日 dr 兜底，只能提供 dr，明细置 0
        mask = np.abs(bars["dr"].astype("float64") - 1.0) > 1e-9
        return [(int(t) * 1000, 0.0, 0.0, 0.0, 0.0, 0.0, 0, float(r))
                for t, r in zip(bars["time"][mask], bars["dr"][mask])]

    def _export_divid(self) -> Optional[Dict[str, List[DividEvent]]]:
        """读取内置策略导出的除权 JSON，转换为与 DividData 相同的事件元组。"""
        raw = self._load_export_json("meta/divid_factors.json")
        if raw is None:
            return None
        cached_raw, cached_events = self._export_events
        if cached_raw is raw:
            return cached_events
        events = {code: [(int(e[0]), float(e[1]), float(e[2]), float(e[3]), float(e[4]), float(e[5]),
                          int(e[6]), float(e[7])) for e in rows] for code, rows in raw.items()}
        self._export_events = (raw, events)
        return events

    def _dividdata(self) -> Optional[Dict[str, List[DividEvent]]]:
        """返回 DividData 解析结果；按间隔检查源目录是否有新版本，有则复制到缓存并重新解析。"""
        # 无论上次成功与否都按间隔重试，避免源目录缺失或复制失败时每个请求都重复复制、重复告警
        if time.time() - self._dividdata_checked < RELOAD_CHECK_SECONDS:
            return self._dividdata_events
        with self._lock:
            if time.time() - self._dividdata_checked >= RELOAD_CHECK_SECONDS:
                self._refresh_dividdata()
                self._dividdata_checked = time.time()
        return self._dividdata_events

    def _refresh_dividdata(self) -> None:
        """复制 DividData 到缓存目录并解析；源目录正在被写或复制期间发生变化时放弃，下次再试。"""
        source = os.path.join(self.config.datadir, "DividData")
        cache = os.path.join(self.config.divid_cache_dir, "DividData")
        try:
            signature = _dir_signature(source)
        except OSError as exc:
            logger.warning(f"[本地行情] 读取 DividData 目录失败: {exc}")
            signature = None
        if signature is not None and signature != self._dividdata_signature:
            newest = max(item[2] for item in signature) / 1e9
            if time.time() - newest < DIVIDDATA_QUIET_SECONDS:
                logger.info("[本地行情] DividData 刚被修改，稍后再复制")
            else:
                try:
                    events = _copy_and_parse_dividdata(source, cache, signature)
                except Exception as exc:  # 复制或解析失败时保留上一份结果
                    logger.warning(f"[本地行情] DividData 更新失败，继续使用上一份: {exc!r}")
                else:
                    self._dividdata_events = events
                    self._dividdata_signature = signature
                    logger.info(f"[本地行情] DividData 已更新: 品种数={len(events)}")
                    return
        if self._dividdata_events is None and os.path.isdir(cache):
            # 首次加载且源目录不可用时，使用上次复制好的缓存
            try:
                self._dividdata_events = parse_dividdata(cache)
                logger.info(f"[本地行情] 使用 DividData 缓存: {cache}")
            except Exception as exc:
                logger.warning(f"[本地行情] DividData 缓存解析失败: {exc!r}")

    def _compare_divid_sources(self, export_events: Optional[dict], dividdata_events: Optional[dict]) -> None:
        """两个除权来源每次加载新版本后对比一次，有分歧时记录警告，便于发现盘中修正或数据过期。"""
        if export_events is None or dividdata_events is None:
            return
        versions = (id(export_events), id(dividdata_events))
        if versions == self._compared_versions:
            return
        self._compared_versions = versions
        differing = [code for code, rows in export_events.items()
                     if code in dividdata_events and not _same_events(rows, dividdata_events[code])]
        only_export = [code for code in export_events if code not in dividdata_events]
        if differing or only_export:
            logger.warning(f"[本地行情] 除权来源不一致: 数据不同 {len(differing)} 个 {differing[:10]}，"
                           f"仅导出文件有 {len(only_export)} 个 {only_export[:10]}；"
                           f"当前以 {self.config.divid_source} 为准")
        else:
            logger.info(f"[本地行情] 除权来源一致: 品种数={len(export_events)}")

    # ---------- 财务数据 ----------

    def get_financial_data(self, stock_list: Sequence[str], table_list: Sequence[str] = (), start_time: Any = "",
                           end_time: Any = "", report_type: str = "report_time") -> Dict[str, Dict[str, pd.DataFrame]]:
        """返回财务数据 {代码: {表名: DataFrame}}，目前只支持股本表 Capital。

        直接解析 <datadir>/Finance/<SH|SZ>/86400/<代码>_7004.DAT（由内置策略调用 download_history_data(代码, "Capital") 更新），
        DataFrame 列与 xtdata 一致，m_timetag（报告截止日）/ m_anntime（公告日）为 YYYYMMDD 字符串。

        Args:
            stock_list: 代码列表；没有股本数据的代码返回空 DataFrame（与 xtdata 一致）。
            table_list: 表名列表，支持 Capital（大小写不敏感，也接受 CAPITALSTRUCTURE）；为空时只返回 Capital。
            start_time / end_time: 闭区间日期过滤。
            report_type: report_time 按报告截止日过滤，announce_time 按公告日过滤。

        Raises:
            ValueError: 请求了 Capital 以外的表，或 report_type 非法。
        """
        tables = list(table_list) or ["Capital"]
        unsupported = [t for t in tables if str(t).upper() not in ("CAPITAL", "CAPITALSTRUCTURE")]
        if unsupported:
            raise ValueError(f"本地行情模式的 get_financial_data 只支持 Capital 表，收到 {unsupported}")
        if report_type not in ("report_time", "announce_time"):
            raise ValueError(f"report_type 只能是 report_time 或 announce_time，收到 {report_type!r}")
        start, end = _normalize_date(start_time), _normalize_date(end_time)
        date_column = "m_timetag" if report_type == "report_time" else "m_anntime"
        result: Dict[str, Dict[str, pd.DataFrame]] = {}
        for code in stock_list:
            frame = self._capital_frame(code)
            if start:
                frame = frame[frame[date_column] >= start]
            if end:
                frame = frame[frame[date_column] <= end]
            frame = frame.reset_index(drop=True)
            result[code] = {table: frame.copy() for table in tables}  # 键名沿用调用方写法，与 xtdata 一致
        return result

    def _capital_frame(self, code: str) -> pd.DataFrame:
        """读取单只股票的股本表 DAT，转换为 xtdata 的 Capital DataFrame；文件不存在时返回空表。"""
        parts = str(code).split(".")
        path = None
        if len(parts) == 2:
            path = os.path.join(self.config.datadir, "Finance", parts[1].upper(), "86400", f"{parts[0]}_7004.DAT")
        if path is None or not os.path.exists(path):
            return pd.DataFrame(columns=CAPITAL_COLUMNS)
        with open(path, "rb") as fh:
            buf = fh.read()
        records = np.frombuffer(buf, CAPITAL_DTYPE, count=len(buf) // CAPITAL_DTYPE.itemsize)
        timetag = records["m_timetag"].astype("int64")
        anntime = records["m_anntime"].astype("int64")
        anntime = np.where(anntime > 0, anntime, timetag)  # 与 xtdata 一致：缺公告日时用报告截止日
        frame = pd.DataFrame({
            "m_timetag": _bar_dates(timetag // 1000),
            "m_anntime": _bar_dates(anntime // 1000),
            "total_capital": records["total_capital"],
            "circulating_capital": records["circulating_capital"],
            "restrict_circulating_capital": records["restrict_circulating_capital"],
            "freeFloatCapital": records["freeFloatCapital"],
            "m_quarter": records["m_quarter"].astype("float64"),
        }, columns=CAPITAL_COLUMNS)
        return frame

    # ---------- 板块与品种信息 ----------

    def get_sector_list(self) -> List[str]:
        """返回内置策略导出的板块名称列表。"""
        return list(self._sectors())

    def get_stock_list_in_sector(self, sector_name: str, real_timetag: Any = -1) -> List[str]:
        """返回板块成份；本地只有最新成份，real_timetag 忽略；未知板块返回空列表（与 xtdata 一致）。"""
        return list(self._sectors().get(sector_name, []))

    def get_instrument_detail(self, stock_code: str, iscomplete: bool = False) -> Optional[Dict[str, Any]]:
        """返回品种信息字典，找不到时返回 None；字段为内置 get_instrument_detail 的结果。"""
        detail = self._instruments().get(stock_code)
        return dict(detail) if detail is not None else None

    def get_instrument_detail_list(self, stock_list: Sequence[str],
                                   iscomplete: bool = False) -> Dict[str, Optional[Dict[str, Any]]]:
        """批量返回品种信息，行为与 xtdata.get_instrument_detail_list 一致。"""
        return {code: self.get_instrument_detail(code, iscomplete) for code in stock_list}

    def _sectors(self) -> Dict[str, List[str]]:
        return self._load_export_json("meta/sectors.json") or {}

    def _instruments(self) -> Dict[str, Dict[str, Any]]:
        return self._load_export_json("meta/instruments.json") or {}

    # ---------- 导出文件与更新状态 ----------

    def _load_export_json(self, rel_path: str) -> Optional[Any]:
        """读取导出目录下的 JSON，按修改时间缓存；文件不存在或损坏时返回上一份（没有则 None）。"""
        # 文件缺失时同样按间隔重试，避免每个请求都告警
        now = time.time()
        cached = self._json_cache.get(rel_path)
        if now - self._json_checked.get(rel_path, 0.0) < RELOAD_CHECK_SECONDS:
            return cached[1] if cached is not None else None
        with self._lock:
            self._json_checked[rel_path] = now
            path = os.path.join(self.config.export_dir, rel_path)
            try:
                mtime = os.path.getmtime(path)
                if cached is not None and cached[0] == mtime:
                    return cached[1]
                with open(path, "r", encoding="utf-8") as fh:
                    data = json.load(fh)
            except (OSError, ValueError) as exc:
                if cached is None:
                    logger.warning(f"[本地行情] 读取 {rel_path} 失败: {exc!r}")
                return cached[1] if cached is not None else None
            self._json_cache[rel_path] = (mtime, data)
            return data

    def _check_update_status(self) -> None:
        """检查内置策略的更新状态，正在更新、失败或数据落后时记录警告（状态变化时才记录）。"""
        now = time.time()
        if now - self._status_checked < RELOAD_CHECK_SECONDS:
            return
        self._status_checked = now
        message = self.describe_update_status()
        if message != self._last_status_message:
            self._last_status_message = message
            log = logger.info if message.startswith("数据已就绪") else logger.warning
            log(f"[本地行情] {message}")

    def get_adjust_samples(self, dividend_type: str = "front_ratio",
                           stock_list: Sequence[str] = ()) -> Dict[str, pd.DataFrame]:
        """返回内置策略导出的 QMT 复权样本（非 xtdata 接口），供同步程序核对自算的复权结果。

        Args:
            dividend_type: front_ratio / back_ratio / front / back。
            stock_list: 只返回这些代码；为空时返回全部样本代码。

        Returns:
            {代码: DataFrame}，index 为日期 YYYYMMDD 字符串，列为 time / open / high / low / close / volume /
            amount / preClose（QMT 原值）。样本目录不存在时返回空字典。

        Raises:
            ValueError: dividend_type 不支持。
        """
        if dividend_type not in ADJUST_SAMPLE_TYPES:
            raise ValueError(f"dividend_type 只能是 {'/'.join(ADJUST_SAMPLE_TYPES)}，收到 {dividend_type!r}")
        folder = os.path.join(self.config.export_dir, "validation", dividend_type)
        if not os.path.isdir(folder):
            return {}
        wanted = {str(code) for code in stock_list}
        result: Dict[str, pd.DataFrame] = {}
        for name in sorted(os.listdir(folder)):
            code = name[:-4]
            if not name.endswith(".csv") or (wanted and code not in wanted):
                continue
            result[code] = pd.read_csv(os.path.join(folder, name), dtype={"date": str}).set_index("date")
        return result

    def get_update_status(self) -> Dict[str, Any]:
        """返回内置策略的更新状态，供同步程序判断数据是否可以入库。

        Returns:
            {"backend": "local", "status": status.json 内容, "ready": ready.json 内容, "message": 一句话描述}；
            文件不存在时对应项为 None。status.state 为 done 且 ready.trade_date 为最新交易日时表示日线已就绪。
        """
        # 两个文件只读一次，保证 status、ready 与 message 对应同一时刻的内容
        status = self._read_small_json("status.json")
        ready = self._read_small_json("ready.json")
        return {"backend": "local", "status": status, "ready": ready,
                "message": self._describe_status(status or {}, ready or {})}

    def describe_update_status(self) -> str:
        """读取 status.json / ready.json，返回一句话的更新状态描述。"""
        return self._describe_status(self._read_small_json("status.json") or {},
                                     self._read_small_json("ready.json") or {})

    @staticmethod
    def _describe_status(status: Dict[str, Any], ready: Dict[str, Any]) -> str:
        """根据 status.json、ready.json 的内容生成一句话的更新状态描述。"""
        if status.get("state") == "running":
            return f"大QMT 内置策略正在更新数据（开始于 {status.get('started_at')}），DAT 可能处于更新中"
        if status.get("state") == "failed":
            return f"大QMT 内置策略最近一次更新失败（{status.get('finished_at')}），请查看导出目录 logs/"
        trade_date = str(ready.get("trade_date", ""))
        if not trade_date:
            return "未找到 ready.json，无法确认数据是否已更新"
        expected = _expected_trade_date(dt.datetime.now(dt.timezone(dt.timedelta(hours=8))))
        if trade_date < expected:
            return f"数据可能未更新：最近一次成功更新的交易日为 {trade_date}，预期 {expected}（节假日可忽略）"
        return f"数据已就绪：交易日 {trade_date}，完成于 {ready.get('finished_at')}"

    def _read_small_json(self, rel_path: str) -> Optional[Dict[str, Any]]:
        """直接读取小状态文件，不缓存。"""
        try:
            with open(os.path.join(self.config.export_dir, rel_path), "r", encoding="utf-8") as fh:
                return json.load(fh)
        except (OSError, ValueError):
            return None

    # ---------- 启动自检 ----------

    def run_self_check(self) -> Dict[str, float]:
        """用内置策略导出的 QMT front_ratio 结果对照本地计算，返回 {代码: 最大相对误差}。

        自检同时覆盖 DAT 解析、除权来源与复权算法；误差超过 1e-6 时记录警告。
        """
        result: Dict[str, float] = {}
        folder = os.path.join(self.config.export_dir, "validation", "front_ratio")
        if not os.path.isdir(folder):
            logger.warning(f"[本地行情] 未找到自检目录 {folder}，跳过复权自检")
            return result
        for name in sorted(os.listdir(folder)):
            if not name.endswith(".csv"):
                continue
            code = name[:-4]
            try:
                expected = pd.read_csv(os.path.join(folder, name), dtype={"date": str}).set_index("date")
                actual = self.get_market_data_ex(PRICE_FIELDS, [code], dividend_type="front_ratio")[code]
                joined = expected[PRICE_FIELDS].join(actual.add_suffix("_local"), how="inner")
            except Exception as exc:  # 自检只用于提示，单个样本出错不能影响服务启动
                logger.warning(f"[本地行情] 复权自检 {code} 执行失败: {exc!r}")
                result[code] = float("inf")
                continue
            if joined.empty:
                result[code] = float("inf")
                continue
            errors = [np.nanmax(np.abs(joined[f + "_local"] / joined[f] - 1)) for f in PRICE_FIELDS]
            result[code] = float(np.nanmax(errors))
        bad = {code: err for code, err in result.items() if not err <= 1e-6}
        if bad:
            logger.warning(f"[本地行情] 复权自检存在偏差（自检文件可能早于最新除权）: {bad}")
        else:
            logger.info(f"[本地行情] 复权自检通过: {len(result)} 个品种，最大相对误差 "
                        f"{max(result.values()) if result else 0:.2e}")
        return result


# ==================== 内部工具 ====================

def _dir_signature(folder: str) -> tuple:
    """DividData 目录的文件签名 (文件名, 大小, 修改时间纳秒)；LOCK 文件被大QMT 占用，不参与。"""
    items = []
    for name in sorted(os.listdir(folder)):
        if name == "LOCK" or not (name == "CURRENT" or name.startswith("MANIFEST-")
                                  or name.endswith((".ldb", ".sst", ".log"))):
            continue
        stat = os.stat(os.path.join(folder, name))
        items.append((name, stat.st_size, stat.st_mtime_ns))
    if not items:
        raise OSError(f"{folder} 中没有 LevelDB 文件")
    return tuple(items)


def _copy_and_parse_dividdata(source: str, cache: str, signature: tuple) -> Dict[str, List[DividEvent]]:
    """复制到临时目录 -> 确认复制期间源目录未变化 -> 解析成功后替换缓存目录。"""
    tmp = cache + ".tmp"
    shutil.rmtree(tmp, ignore_errors=True)
    os.makedirs(tmp)
    for name, _, _ in signature:
        shutil.copy2(os.path.join(source, name), os.path.join(tmp, name))
    if _dir_signature(source) != signature:
        shutil.rmtree(tmp, ignore_errors=True)
        raise RuntimeError("复制期间 DividData 发生变化")
    events = parse_dividdata(tmp)
    shutil.rmtree(cache, ignore_errors=True)
    os.replace(tmp, cache)
    return events


def _same_events(left: List[DividEvent], right: List[DividEvent]) -> bool:
    """判断两组除权事件是否一致（浮点按 1e-9 容差比较）。"""
    if len(left) != len(right):
        return False
    return all(abs(a - b) <= 1e-9 for x, y in zip(left, right) for a, b in zip(x, y))


def _expected_trade_date(now: dt.datetime) -> str:
    """按工作日估算当前应已更新到的交易日：工作日 DATA_READY_TIME 之后为当天，否则为上一个工作日。

    不含节假日日历，节假日期间可能误报"数据落后"，仅用于日志提示。
    """
    day = now.date()
    if not (day.weekday() < 5 and now.time() >= DATA_READY_TIME):
        day -= dt.timedelta(days=1)
        while day.weekday() >= 5:
            day -= dt.timedelta(days=1)
    return day.strftime("%Y%m%d")

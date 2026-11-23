#coding:gbk
"""
大QMT 内置 Python 盘中快照导出脚本（xqshare 本地行情模式 get_full_tick 的数据源）。

所属层次：
    运行在大QMT 内置 Python 中的定时导出任务。只使用大QMT 内置接口，不 import xtquant，不使用线程或多进程，
    只写文件。与 qmt_data_exporter.py（盘后日线导出）相互独立，作为两个策略分别运行。

背景：
    xqshare 本地行情模式读不到实时行情。本脚本在 SCHEDULE_TIMES 的每个时间对全市场调用 ContextInfo.get_full_tick，
    把返回结果原样保存为 tick/full_tick.json；xqshare 本地模式的 get_full_tick 读取该文件返回。
    文件只保留最新一份，每次整体覆盖。

主要流程（每个定时时间执行一次）：
    1. 取 TICK_SECTORS 各板块成份，合并去重为全市场代码；
    2. 分批调用 get_full_tick（每批 BATCH_SIZE 只），合并结果；返回为空时不覆盖上一份文件；
    3. 写 tick/full_tick.json（先写临时文件再 os.replace，xqshare 不会读到写了一半的文件）。

输出文件 tick/full_tick.json：
    {"generated_at": "YYYY-MM-DD HH:MM:SS", "trigger": 触发方式, "seconds": 用时,
     "sectors": {板块名: 成份数}, "count": 快照数, "ticks": {代码: get_full_tick 原始结果}}

定时：
    init 注册一个每 CHECK_SECONDS 秒运行一次的检查器，每到 SCHEDULE_TIMES 中的一个时间执行一次导出。
    策略启动时已经过去的时间点不补做（RUN_ON_START=True 时启动后先导出一次）。
    大QMT 所有策略在同一线程中执行：qmt_data_exporter.py 在它的 SCHEDULE_TIME（默认 15:45）运行的 5~7 分钟内，
    本脚本的定时会被推迟到它结束之后，时间点尽量不要安排在这段时间内。

使用：
    1. 修改下方"手动配置区"：EXPORT_ROOT 与 qmt_data_exporter.py 相同；SCHEDULE_TIMES 按需增减；
    2. 大QMT 新建 Python 策略，粘贴全部内容，主图选任意股票日线，点"运行"（不要用"回测"，定时器在回测中无效）；
    3. 日志打印在 QMT 日志窗口，同时追加到 EXPORT_ROOT/logs/<日期>.log（与日线导出共用，行首标记 [tick]）。
"""

import datetime
import json
import os
import time
import traceback
from typing import Any, Tuple

# ======================= 手动配置区 =======================

# 输出根目录：与 qmt_data_exporter.py 相同（xqshare 服务端通过环境变量指定该目录）
EXPORT_ROOT = r"E:\swhyqmt\export"
# 每天导出快照的时间（HH:MM:SS，按需增减）；收盘后保留一个时间点，最后一份快照即为收盘快照
SCHEDULE_TIMES = ["09:35:00", "10:30:00", "11:25:00", "13:30:00", "14:30:00", "14:55:00", "15:05:00"]
# 策略启动后是否立即导出一次
RUN_ON_START = True
# 全市场范围：这些板块成份的并集（与 qmt_data_exporter.py 的品种池一致）
TICK_SECTORS = ["沪深A股", "沪深ETF", "沪深指数"]
# 每次调用 get_full_tick 的代码数
BATCH_SIZE = 1000
# 定时检查间隔（秒）：实际导出时间最多比 SCHEDULE_TIMES 晚这么多
CHECK_SECONDS = 30
# 输出文件（相对 EXPORT_ROOT）
TICK_FILE = "tick/full_tick.json"
# 替换输出文件失败时的重试次数与间隔（秒）：Windows 上 xqshare 正在读该文件时替换会被拒绝
REPLACE_ATTEMPTS = 5
REPLACE_WAIT_SECONDS = 0.5

# ======================= 全局状态 =======================

_STARTED = False          # 是否已处理过启动执行
_DONE = set()             # 今天已执行（或启动时已过去）的时间点 {(YYYYMMDD, HH:MM:SS)}


def init(C: Any) -> None:
    """注册定时检查器；启动时已经过去的时间点标记为已执行，不补做。"""
    today, clock = now_parts()
    for item in SCHEDULE_TIMES:
        if item <= clock:
            _DONE.add((today, item))
    # 起始时间设为过去的时间，定时器立即开始
    start = (datetime.datetime.now() - datetime.timedelta(seconds=CHECK_SECONDS)).strftime("%Y-%m-%d %H:%M:%S")
    log("init: EXPORT_ROOT={}, SCHEDULE_TIMES={}".format(EXPORT_ROOT, SCHEDULE_TIMES))
    try:
        C.run_time("check_schedule", "{}nSecond".format(CHECK_SECONDS), start, "SH")
        log("已注册定时检查: 每 {} 秒".format(CHECK_SECONDS))
    except Exception:
        log("注册定时检查失败，只能启动时执行一次:\n" + traceback.format_exc())


def handlebar(C: Any) -> None:
    """启动后在最后一根 K 线执行一次（RUN_ON_START=True 时）。"""
    global _STARTED
    if _STARTED:
        return
    try:
        if not C.is_last_bar():
            return
    except Exception:
        pass
    _STARTED = True
    if RUN_ON_START:
        export_ticks(C, "启动执行")


def check_schedule(C: Any) -> None:
    """定时检查入口：有到期且今天未执行的时间点时导出一次。"""
    global _DONE
    today, clock = now_parts()
    _DONE = set(item for item in _DONE if item[0] == today)  # 只保留今天的记录
    due = [item for item in sorted(SCHEDULE_TIMES) if item <= clock and (today, item) not in _DONE]
    if not due:
        return
    # 被其他策略推迟时可能同时有多个时间点到期，只导出一次
    for item in due:
        _DONE.add((today, item))
    export_ticks(C, "定时 " + due[-1])


# ======================= 主流程 =======================

def export_ticks(C: Any, trigger: str) -> None:
    """取全市场快照并写 tick/full_tick.json；失败只记日志，保留上一份文件。"""
    begin = time.time()
    try:
        sectors = {}
        codes = set()
        for name in TICK_SECTORS:
            members = list(C.get_stock_list_in_sector(name) or [])
            sectors[name] = len(members)
            codes.update(members)
        codes = sorted(codes)
        ticks = {}
        for start in range(0, len(codes), BATCH_SIZE):
            ticks.update(C.get_full_tick(codes[start:start + BATCH_SIZE]) or {})
        if not ticks:
            raise RuntimeError("get_full_tick 返回为空（代码 {} 只）".format(len(codes)))
        data = {
            "generated_at": now_text(),
            "trigger": trigger,
            "seconds": round(time.time() - begin, 2),
            "sectors": sectors,
            "count": len(ticks),
            "ticks": ticks,
        }
        write_json(TICK_FILE, data)
        log("[OK] {}：代码 {} 只，快照 {} 只，用时 {}s".format(trigger, len(codes), len(ticks), data["seconds"]))
    except Exception:
        log("[FAIL] {}:\n{}".format(trigger, traceback.format_exc()))


# ======================= 工具函数 =======================

def write_json(rel_path: str, obj: Any) -> None:
    """写 JSON：先写临时文件再原子替换；替换被拒绝（xqshare 正在读）时稍等重试。"""
    path = os.path.join(EXPORT_ROOT, rel_path)
    folder = os.path.dirname(path)
    if not os.path.isdir(folder):
        os.makedirs(folder)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, ensure_ascii=False, default=json_default)
    for attempt in range(1, REPLACE_ATTEMPTS + 1):
        try:
            os.replace(tmp, path)
            return
        except PermissionError:
            if attempt == REPLACE_ATTEMPTS:
                raise
            time.sleep(REPLACE_WAIT_SECONDS)


def json_default(value: Any) -> Any:
    """JSON 无法直接序列化的值：numpy 数值、数组转为 Python 数值、列表，其他转为字符串。"""
    if hasattr(value, "tolist"):
        return value.tolist()
    return str(value)


def now_parts() -> Tuple[str, str]:
    """当前日期 YYYYMMDD 与时间 HH:MM:SS。"""
    now = datetime.datetime.now()
    return now.strftime("%Y%m%d"), now.strftime("%H:%M:%S")


def now_text() -> str:
    """当前时间字符串。"""
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(text: str) -> None:
    """打印到 QMT 日志窗口，同时追加到 logs/<日期>.log。"""
    line = "[{}] [tick] {}".format(now_text(), text)
    print(line)
    try:
        log_dir = os.path.join(EXPORT_ROOT, "logs")
        if not os.path.isdir(log_dir):
            os.makedirs(log_dir)
        with open(os.path.join(log_dir, datetime.date.today().strftime("%Y%m%d") + ".log"), "a",
                  encoding="utf-8") as fh:
            fh.write(line + "\n")
    except Exception:
        pass

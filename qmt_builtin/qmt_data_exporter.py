#coding:gbk
"""
大QMT 内置 Python 数据维护脚本（xqshare 数据源）。

所属层次：
    运行在大QMT 内置 Python 中的每日数据维护任务，为 xqshare 服务端准备数据（日线 + 财务数据）。
    只使用大QMT 内置接口，不 import xtquant，不使用线程或进程。

背景：
    miniQMT 已关闭，外部 xtdata 无法连接。xqshare 服务端改为直接读取 datadir 下的 DAT 日线文件。
    大QMT 内置 Python 以后可能被禁止读取外部文件，但仍可写文件，因此本脚本只写不读。

主要流程（大QMT 启动时执行一次，每天 SCHEDULE_TIME 盘后再执行一次）：
    1. 写 status.json（state=running），告诉 xqshare 正在更新 DAT；
    2. 取板块成份，确定品种池（沪深A股 + 沪深ETF + 沪深指数）；
    3. download_history_data 逐只增量补日线到 datadir（xqshare 读的就是这些 DAT）；
    4. 检查最新交易日：只取基准品种 CALENDAR_CODE 的最后一根日线（不对全市场取日线：内置 get_market_data_ex
       即使只取 1 根，也会把该品种全部历史日线读进大QMT 自身的缓存且不释放，全市场约占 3.7GB）；
    5. 导出 DAT 之外的数据（xqshare 不直接解析这些格式）：
       - meta/sectors.json        板块成份
       - meta/instruments.json    品种信息
       - meta/divid_factors.json  除权明细（DividData 是 LevelDB，QMT 运行时加锁，外部不便读取）
       交易日历不导出：内置 get_trading_calendar 只有投研版提供，交易日历从 Tushare 获取。
    6. 导出少量品种的 QMT 四种复权结果到 validation/：front_ratio 供 xqshare 启动自检，
       四种复权供 market-data 每日核对自算的复权表（经 xqshare get_adjust_samples 读取）；
    7. 写 status.json（state=done/failed）；全部成功时再写 ready.json 作为"已更新好"的标记。
       盘中启动执行时当日 K 线未收盘，不写 ready.json，由当天 SCHEDULE_TIME 的定时任务补上；
    8. 日线部分结束后，逐只调用 download_history_data(代码, 表名, "", "") 下载 FINANCIAL_TABLES 财务表到 datadir
       （xqshare 直接解析 Finance/<SH|SZ>/86400/<代码>_7004.DAT）。每只股票整表重新下载，
       实测沪深A股 5224 只股本表约 66 秒、0 失败。结果只记入 status.json 的 financial 字段和日志，
       不改变 state，也不影响 ready.json。

单线程限制（官方文档）：
    所有内置策略在同一个线程中执行，本脚本运行期间（约 5~7 分钟）其他内置策略暂停，因此每天只运行两次。

输出目录结构（EXPORT_ROOT）：
    status.json                最近一次运行状态：state 为 running / done / failed，含各步骤结果和 financial 财务下载结果
    ready.json                 最近一次成功运行：{"trade_date": "YYYYMMDD", "finished_at": ..., ...}
    meta/sectors.json          {板块名: [代码, ...]}
    meta/instruments.json      {代码: get_instrument_detail 结果}
    meta/divid_factors.json    {代码: [[毫秒时间, 每股红利, 送股, 转增, 配股, 配股价, 股改, dr], ...]}
    validation/<复权类型>/<代码>.csv   复权类型为 front_ratio / back_ratio / front / back
    logs/<YYYYMMDD>.log

xqshare 读取约定：
    - status.json 的 state 为 running：DAT 正在更新，继续使用已缓存数据或稍后再读；
    - ready.json 的 trade_date 等于最新交易日：当天数据已就绪；
    - ready.json 的 trade_date 落后：数据未更新，xqshare 照常返回并给出提示。
    所有文件均先写临时文件再 os.replace，保证 xqshare 读到的始终是完整文件。

使用：
    1. 修改下方"手动配置区"（主要是 EXPORT_ROOT 和 SCHEDULE_TIME）；
    2. 大QMT 新建 Python 策略，粘贴全部内容，主图任选股票、日线，点"运行"（不要点"回测"）；
    3. 策略保持运行状态，每天 SCHEDULE_TIME 自动执行；RUN_ON_START=True 时启动后立即执行一次。
    4. 不再需要单独的财务下载策略（原 qmt_financial_downloader.py 已合并到本脚本）。
"""

import datetime
import gc
import json
import os
import sys
import time
import traceback

# ======================= 手动配置区 =======================

# 输出根目录：xqshare 服务端通过环境变量指向这里
EXPORT_ROOT = r"E:\swhyqmt\export"

# 每日定时执行时间（债券/货币 ETF 交易到 15:30，留出日线落地时间）
SCHEDULE_TIME = "15:45:00"

# 策略启动后是否立即执行一次（大QMT 每天启动时补齐数据）
RUN_ON_START = True

# 品种池来源板块：品种池 = 这些板块成份的并集
UNIVERSE_SECTORS = ["沪深A股", "沪深ETF", "沪深指数"]

# 导出成份列表的板块（两个项目实际用到的 + 品种池板块）
EXPORT_SECTORS = ["沪深A股", "上证A股", "深证A股", "创业板", "科创板", "沪深ETF", "沪深指数"]

# 判断最新交易日的基准品种（上证指数每个交易日都有 bar）
CALENDAR_CODE = "000001.SH"

# 复权样本品种：导出 QMT 的复权结果，供 xqshare 自检和 market-data 核对自算的复权表。
# 覆盖现金分红、多次送转、配股、停牌期间多次除权（600519 2006 年）和 ETF 分红
VALIDATE_CODES = ["510300.SH", "159915.SZ", "510050.SH", "600519.SH", "601318.SH", "300750.SZ",
                  "000001.SZ", "000002.SZ", "600036.SH", "600000.SH"]

# 复权样本导出的复权类型（front_ratio 必须保留：xqshare 启动自检使用）
VALIDATE_DIVIDEND_TYPES = ["front_ratio", "back_ratio", "front", "back"]

# 自检导出字段
VALIDATE_FIELDS = ["time", "open", "high", "low", "close", "volume", "amount", "preClose"]

# 财务数据：下载范围板块（只有股票有财务数据）与财务表；FINANCIAL_TABLES 为空列表时不下载财务数据
# 可选表：Balance / Income / CashFlow / Capital / HolderNum / Top10Holder / Top10FlowHolder / PershareIndex
# xqshare 目前只解析 Capital；每多一张表，下载时间约增加一倍
FINANCIAL_SECTOR = "沪深A股"
FINANCIAL_TABLES = ["Capital"]

# ======================= 全局状态 =======================

_STARTED = False          # 是否已处理过启动执行
_LAST_RUN_DATE = ""       # 最近一次在 SCHEDULE_TIME 之后成功执行的日期，避免同一天重复执行
_RUNNING = False          # 防止定时任务重入


def init(C):
    """注册每日定时任务。"""
    log("init: EXPORT_ROOT={}, SCHEDULE_TIME={}".format(EXPORT_ROOT, SCHEDULE_TIME))
    start = "{} {}".format(datetime.date.today().strftime("%Y-%m-%d"), SCHEDULE_TIME)
    try:
        C.run_time("scheduled_job", "1nDay", start, "SH")
        log("已注册每日定时任务: 首次 {}".format(start))
    except Exception:
        log("注册定时任务失败，只能依靠启动执行:\n" + traceback.format_exc())


def handlebar(C):
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
        run_update(C, "启动执行")


def scheduled_job(C):
    """定时任务入口：同一天在 SCHEDULE_TIME 之后已成功执行过则跳过（盘中的启动执行不算）。"""
    if _LAST_RUN_DATE == datetime.date.today().strftime("%Y%m%d"):
        log("今天已执行过，跳过定时任务")
        return
    run_update(C, "定时执行")


# ======================= 主流程 =======================

def run_update(C, trigger):
    """执行一次完整更新：先标记 running，日线部分结束时写 status.json / ready.json，然后下载财务数据。"""
    global _RUNNING
    if _RUNNING:
        log("上一次更新仍在进行，跳过")
        return
    _RUNNING = True
    begin = time.time()
    status = {"state": "running", "trigger": trigger, "started_at": now_text(), "steps": {}, "ok": False}
    log("开始前 " + memory_text())
    try:
        for sub in ["meta", "logs"] + ["validation/" + t for t in VALIDATE_DIVIDEND_TYPES]:
            os.makedirs(os.path.join(EXPORT_ROOT, sub), exist_ok=True)
        write_json("status.json", status)
        log("===== 开始更新（{}）=====".format(trigger))

        sectors = {}
        run_step(status, "sectors", lambda: export_sectors(C, sectors))
        universe = build_universe(sectors)
        status["universe_size"] = len(universe)
        run_step(status, "download", lambda: download_klines(universe))
        check = run_step(status, "check", lambda: check_latest(C)) or {}
        run_step(status, "divid_factors", lambda: export_divid_factors(C, universe))
        run_step(status, "instruments", lambda: export_instruments(C, universe))
        run_step(status, "validation", lambda: export_validation(C))

        status["trade_date"] = check.get("trade_date", "")
        status["ok"] = all(step["ok"] for step in status["steps"].values())
    except Exception:
        status["error"] = traceback.format_exc()
        log("更新异常:\n" + status["error"])
    finally:
        try:
            try:
                finish_update(status, begin)
            except Exception:
                log("写入运行状态失败:\n" + traceback.format_exc())
            # 财务数据放在 ready.json 之后：失败或耗时都不影响 xqshare 判断日线是否就绪
            update_financial(C, status)
        finally:
            # 无论上面是否出错都要复位，否则之后每天的定时任务都会被当成"仍在进行"而跳过
            _RUNNING = False
            gc.collect()  # 先回收再记录，看脚本数据释放后进程内存是否回落
            log("全部结束并回收后 " + memory_text())


def finish_update(status, begin):
    """写 status.json；成功时按是否已收盘决定写 ready.json 和标记"今天已执行"。

    - 在 SCHEDULE_TIME 之前启动、且数据已到今天：当日 K 线尚未收盘，不写 ready.json，
      也不标记今天已执行，保证当天的定时任务照常运行；
    - 在 SCHEDULE_TIME 之前启动、数据只到上一交易日（如盘前启动）：上一交易日数据完整，写 ready.json；
    - 在 SCHEDULE_TIME 之后启动：写 ready.json，并标记今天已执行。
    """
    global _LAST_RUN_DATE
    today = datetime.date.today().strftime("%Y%m%d")
    after_schedule = status["started_at"][11:] >= SCHEDULE_TIME
    intraday = status["ok"] and status.get("trade_date") == today and not after_schedule
    status["state"] = "done" if status["ok"] else "failed"
    status["ready_written"] = status["ok"] and not intraday
    status["finished_at"] = now_text()
    status["seconds"] = round(time.time() - begin, 1)
    write_json("status.json", status)
    if status["ready_written"]:
        write_ready(status)
    if intraday:
        log("盘中执行：当日 K 线尚未收盘，不更新 ready.json，等待 {} 定时执行".format(SCHEDULE_TIME))
    if status["ok"] and after_schedule:
        _LAST_RUN_DATE = today
    log("===== 日线更新结束 state={} trade_date={} 用时 {}s =====".format(
        status["state"], status.get("trade_date", ""), status["seconds"]))


def update_financial(C, status):
    """下载财务数据，结果写入 status["financial"] 后重写 status.json；不改变 state，也不改动 ready.json。"""
    if not FINANCIAL_TABLES:
        return
    begin = time.time()
    log("===== 开始下载财务数据: 板块 {} 表 {} =====".format(FINANCIAL_SECTOR, FINANCIAL_TABLES))
    try:
        info = download_financial(C)
        status["financial"] = dict(ok=True, seconds=round(time.time() - begin, 1), **info)
        log("[OK] financial {}".format(info))
    except Exception:
        status["financial"] = {"ok": False, "seconds": round(time.time() - begin, 1),
                               "error": traceback.format_exc()}
        log("[FAIL] financial\n{}".format(status["financial"]["error"]))
    try:
        write_json("status.json", status)
    except Exception:
        log("写入财务下载结果失败:\n" + traceback.format_exc())
    log("===== 财务数据下载结束 用时 {}s =====".format(status["financial"]["seconds"]))


def run_step(status, name, func):
    """执行单个步骤并记录结果；步骤失败不影响后续步骤。"""
    begin = time.time()
    try:
        result = func()
        info = result if isinstance(result, dict) else {}
        status["steps"][name] = dict(ok=True, seconds=round(time.time() - begin, 1), **info)
        log("[OK] {} {} {}".format(name, info, memory_text()))
        return result
    except Exception:
        status["steps"][name] = {"ok": False, "seconds": round(time.time() - begin, 1),
                                 "error": traceback.format_exc()}
        log("[FAIL] {} {}\n{}".format(name, memory_text(), status["steps"][name]["error"]))
        return None


def write_ready(status):
    """写"已更新好"标记；只在全部步骤成功时调用。"""
    steps = status["steps"]
    ready = {
        "trade_date": status.get("trade_date", ""),
        "finished_at": status["finished_at"],
        "trigger": status["trigger"],
        "universe_size": status.get("universe_size", 0),
        "download_failed": steps.get("download", {}).get("failed", 0),
    }
    write_json("ready.json", ready)


# ======================= 各步骤 =======================

def export_sectors(C, sectors):
    """导出板块成份到 meta/sectors.json；成份同时填入 sectors 字典供构建品种池。"""
    for name in EXPORT_SECTORS:
        sectors[name] = list(C.get_stock_list_in_sector(name) or [])
    empty = [name for name, codes in sectors.items() if not codes]
    if empty:
        raise RuntimeError("板块成份为空: {}".format(empty))
    write_json("meta/sectors.json", sectors)
    return {name: len(codes) for name, codes in sectors.items()}


def build_universe(sectors):
    """品种池 = UNIVERSE_SECTORS 成份并集，排序保证每次处理顺序一致。"""
    codes = set()
    for name in UNIVERSE_SECTORS:
        codes.update(sectors.get(name, []))
    return sorted(codes)


def download_klines(universe):
    """逐只增量补充日线到 datadir；start_time/end_time 为空时 QMT 只补本地缺少的部分。"""
    if not universe:
        raise RuntimeError("品种池为空")
    failed = []
    for idx, code in enumerate(universe, 1):
        try:
            download_history_data(code, "1d", "", "")  # noqa: F821 内置全局函数
        except Exception as exc:
            failed.append("{}: {!r}".format(code, exc))
        if idx % 1000 == 0:
            log("download 进度 {}/{} {}".format(idx, len(universe), memory_text()))
    return {"total": len(universe), "failed": len(failed), "failed_sample": failed[:20]}


def download_financial(C):
    """逐只、逐表下载财务数据到 datadir；表名作为周期传给内置 download_history_data，单只失败继续。"""
    codes = sorted(C.get_stock_list_in_sector(FINANCIAL_SECTOR) or [])
    if not codes:
        raise RuntimeError("板块 {} 成份为空".format(FINANCIAL_SECTOR))
    failed = []
    for idx, code in enumerate(codes, 1):
        for table in FINANCIAL_TABLES:
            try:
                download_history_data(code, table, "", "")  # noqa: F821 内置全局函数，表名作为周期
            except Exception as exc:
                failed.append("{} {}: {!r}".format(code, table, exc))
        if idx % 1000 == 0:
            log("financial 进度 {}/{} {}".format(idx, len(codes), memory_text()))
    return {"total": len(codes), "tables": list(FINANCIAL_TABLES), "failed": len(failed),
            "failed_sample": failed[:20]}


def check_latest(C):
    """用基准品种（上证指数，每个交易日都有 bar）的最后一根日线确定最新交易日。

    只取这一只：内置 get_market_data_ex 即使只取 1 根，也会把品种的全部历史日线读进大QMT 的缓存且不释放，
    对全市场逐只取会让大QMT 进程多占约 3.7GB 内存。各品种是否更新到最新交易日由 market-data 入库后在库中判断。
    """
    cal = fetch_klines(C, [CALENDAR_CODE], ["time"], count=1).get(CALENDAR_CODE)
    if cal is None or cal.empty:
        raise RuntimeError("取不到 {} 的日线，无法确定最新交易日".format(CALENDAR_CODE))
    return {"trade_date": str(cal.index[-1])[:8]}


def export_divid_factors(C, universe):
    """导出全部品种的除权数据；指数等无除权的品种不写入。"""
    result = {}
    failed = []
    for code in universe:
        try:
            raw = C.get_divid_factors(code)
        except Exception as exc:
            failed.append("{}: {!r}".format(code, exc))
            continue
        if raw:
            # 键为除权日毫秒时间戳，值依次为：每股红利、送股、转增、配股、配股价、股改标志、dr
            result[code] = [[int(k)] + list(v) for k, v in sorted(raw.items(), key=lambda kv: int(kv[0]))]
    write_json("meta/divid_factors.json", result)
    return {"codes_with_events": len(result), "failed": len(failed), "failed_sample": failed[:20]}


def export_instruments(C, universe):
    """导出品种信息（名称、上市日、涨跌停价、股本等）。"""
    result = {}
    failed = 0
    for code in universe:
        try:
            detail = C.get_instrument_detail(code)
        except Exception:
            failed += 1
            continue
        if detail:
            result[code] = detail
    write_json("meta/instruments.json", result)
    return {"count": len(result), "failed": failed}


def export_validation(C):
    """导出样本品种的 QMT 各复权类型完整日线，供 xqshare 自检和 market-data 核对复权计算。"""
    result = {}
    for dividend_type in VALIDATE_DIVIDEND_TYPES:
        data = fetch_klines(C, VALIDATE_CODES, VALIDATE_FIELDS, count=-1, dividend_type=dividend_type)
        out_dir = os.path.join(EXPORT_ROOT, "validation", dividend_type)
        written = 0
        for code in VALIDATE_CODES:
            df = data.get(code)
            if df is not None and not df.empty:
                out = df.reset_index(drop=False)
                out = out.rename(columns={out.columns[0]: "date"})
                out["date"] = out["date"].astype(str).str[:8]
                write_csv_atomic(os.path.join(out_dir, code + ".csv"), out[["date"] + VALIDATE_FIELDS])
                written += 1
        result[dividend_type] = written
    return result


# ======================= 取数与文件写入 =======================

def fetch_klines(C, codes, fields, count, dividend_type="none"):
    """调用内置 get_market_data_ex 取日线；subscribe=False 避免订阅数量限制。"""
    return C.get_market_data_ex(fields, list(codes), period="1d", start_time="", end_time="",
                                count=count, dividend_type=dividend_type, fill_data=False, subscribe=False)


def write_csv_atomic(path, df):
    """先写临时文件再替换，避免 xqshare 读到写了一半的文件。"""
    tmp = path + ".tmp"
    df.to_csv(tmp, index=False, encoding="utf-8")
    os.replace(tmp, path)


def write_json(rel_path, obj):
    """写 JSON（原子替换）。"""
    path = os.path.join(EXPORT_ROOT, rel_path)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, ensure_ascii=False, default=str)
    os.replace(tmp, path)


def memory_text():
    """大QMT 进程当前内存和 Python 持有的内存块数（内置 Python 运行在大QMT 进程内，读到的就是大QMT 的内存）。

    工作集：进程当前占用的物理内存（含映射进内存的数据文件，任务管理器"内存"列与之接近）；
    私有：进程自己申请的内存（不含映射文件）；Python 内存块：Python 对象占用的小块数量，脚本数据释放后应回落。
    对照各步骤前后的变化可以区分：Python 内存块回落而进程内存不降，说明占用来自大QMT 自身，不是脚本没有释放。
    """
    blocks = "Python 内存块 {:.1f}万".format(sys.getallocatedblocks() / 10000.0)
    try:
        import ctypes
        from ctypes import wintypes

        class Counters(ctypes.Structure):
            _fields_ = [("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD),
                        ("PeakWorkingSetSize", ctypes.c_size_t), ("WorkingSetSize", ctypes.c_size_t),
                        ("QuotaPeakPagedPoolUsage", ctypes.c_size_t), ("QuotaPagedPoolUsage", ctypes.c_size_t),
                        ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t), ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                        ("PagefileUsage", ctypes.c_size_t), ("PeakPagefileUsage", ctypes.c_size_t)]

        counters = Counters()
        counters.cb = ctypes.sizeof(counters)
        kernel32 = ctypes.windll.kernel32
        kernel32.GetCurrentProcess.restype = wintypes.HANDLE
        getter = ctypes.windll.psapi.GetProcessMemoryInfo
        getter.argtypes = [wintypes.HANDLE, ctypes.POINTER(Counters), wintypes.DWORD]
        if getter(kernel32.GetCurrentProcess(), ctypes.byref(counters), counters.cb):
            return "[内存] 工作集 {:.0f}MB（峰值 {:.0f}MB），私有 {:.0f}MB，{}".format(
                counters.WorkingSetSize / 1048576.0, counters.PeakWorkingSetSize / 1048576.0,
                counters.PagefileUsage / 1048576.0, blocks)
    except Exception:
        pass
    return "[内存] 进程内存读取失败，{}".format(blocks)


def now_text():
    """当前时间字符串。"""
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(text):
    """打印到 QMT 日志窗口，同时追加到 logs/<日期>.log。"""
    line = "[{}] [exporter] {}".format(now_text(), text)
    print(line)
    try:
        log_dir = os.path.join(EXPORT_ROOT, "logs")
        os.makedirs(log_dir, exist_ok=True)
        with open(os.path.join(log_dir, datetime.date.today().strftime("%Y%m%d") + ".log"), "a",
                  encoding="utf-8") as fh:
            fh.write(line + "\n")
    except Exception:
        pass

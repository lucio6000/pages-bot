# -*- coding: utf-8 -*-
"""
Feishu 群机器人 + Facebook Page(粉丝页/主页) 监控（Render 版）
- 群里 @机器人：执行/开始/暂停/中止/抢占执行/间隔=3600/状态/chatid/帮助
- 支持：自动轮询 + 手动触发并存；去重；超时；可中止；本轮结束补跑手动
- 仅把三态写入 result_pages.txt：OK / UNPUBLISHED / NOT_FOUND
- 年龄墙/国家墙：仅日志提示，不计入异常
- “运行异常/权限类”：NEED_TOKEN/AUTH_ERROR/RATE_LIMIT/UNKNOWN/NETWORK_ERROR/TIMEOUT/CANCELLED/WORKER_ERROR
  在飞书推送中单独分区展示（按你指定的样式）
"""

import os, re, json, time, threading, traceback
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, Any, List, Tuple, Optional
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, wait

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from flask import Flask, request, jsonify

# =============== 配置（支持环境变量覆盖） ===============
CONFIG = {
    "FB": {
        "MAPPING_FILE": os.getenv("FB_PAGE_MAPPING_FILE", "pages.txt"),
        "CONCURRENCY": int(os.getenv("FB_CONCURRENCY", "6")),
        "GRAPH_VERSION": os.getenv("FB_GRAPH_VERSION", "v21.0"),
        # 建议：System User 或 Page 的访问令牌；client_pages 场景会自动换 Page Token 读 /settings
        "ACCESS_TOKEN": os.getenv("FB_ACCESS_TOKEN"),
        "NEED_TOKEN_AS_NORMAL": os.getenv("FB_NEED_TOKEN_AS_NORMAL", "false").lower() == "true",
        "OUT_TXT": os.getenv("FB_OUT_TXT", "result_pages.txt"),
        "REQUEST_TIMEOUT": int(os.getenv("FB_REQUEST_TIMEOUT", "6")),
        "MAX_RETRIES": int(os.getenv("FB_MAX_RETRIES", "1")),
        "BACKOFF_FACTOR": float(os.getenv("FB_BACKOFF_FACTOR", "0.5")),
        "ROUND_TIMEOUT": int(os.getenv("FB_ROUND_TIMEOUT", "180")),   # 整轮最多等待秒数
        "FUTURE_EXTRA_GRACE": int(os.getenv("FB_FUTURE_EXTRA_GRACE", "2")),
        # 是否额外探针访问 feed（用来侧写权限/墙）；没 token 时建议打开；有 token 也可保守关闭
        "PROBE_FEED": os.getenv("FB_PROBE_FEED", "true").lower() == "true",
    },
    "FEISHU": {
        "tz": os.getenv("BOT_TZ", "Asia/Shanghai"),
        "require_at": os.getenv("FEISHU_REQUIRE_AT", "true").lower() == "true",
        "domain": os.getenv("FEISHU_DOMAIN", "feishu"),  # 中国区=feishu；国际=lark
        "app_id": os.getenv("FEISHU_APP_ID", ""),
        "app_secret": os.getenv("FEISHU_APP_SECRET", ""),
        "verification_token": os.getenv("FEISHU_VERIFICATION_TOKEN", ""),  # 可留空
        "default_chat_id": os.getenv("FEISHU_DEFAULT_CHAT_ID", ""),
        "push_on_no_abnormal": os.getenv("FEISHU_PUSH_ON_NO_ABNORMAL", "false").lower() == "true",
        "max_text_len": int(os.getenv("FEISHU_MAX_TEXT_LEN", "1800")),
    },
    "SERVER": {
        "host": "0.0.0.0",
        "port": int(os.getenv("PORT", "3000")),
    },
    "SCHEDULE": {
        "interval_seconds": int(os.getenv("BOT_INTERVAL_SECONDS", "3600")),
    }
}
# ======================================================

# =============== 状态分组常量 & 工具 ===============
CERTAIN_ABNORMAL = {"UNPUBLISHED", "NOT_FOUND"}    # 确定异常（会计入“确定异常 清单”）
TECH_EXCEPTIONS  = {"NEED_TOKEN", "AUTH_ERROR", "RATE_LIMIT", "UNKNOWN",
                    "NETWORK_ERROR", "TIMEOUT", "CANCELLED", "WORKER_ERROR"}  # 运行/权限/网络类（单独分区）
IGNORED_STATUSES = {"RESTRICTED_AGE", "RESTRICTED_COUNTRY"}  # 年龄墙/国家墙：仅日志提示
RESULT_KEEP_STATUSES = {"OK", "UNPUBLISHED", "NOT_FOUND"}    # 仅这三态写入结果文件

def _bucket(status: str) -> str:
    s = (status or "").upper()
    if s in CERTAIN_ABNORMAL:      return "certain_abnormal"
    if s in TECH_EXCEPTIONS:       return "tech_exception"
    if s in IGNORED_STATUSES:      return "ignored"
    if s in RESULT_KEEP_STATUSES:  return "tri_state"
    return "other"

def _fmt_label_id(r: Dict[str, Any]) -> str:
    """输出 Label-1234567890；兼容 page_id/app_id 字段"""
    label = (r.get("label") or "").strip()
    rid = r.get("page_id") or r.get("app_id") or ""
    return f"{label}-{rid}".strip("-")

def is_abnormal(row: Dict[str, Any]) -> bool:
    s = (row.get("status") or "").upper()
    return s in CERTAIN_ABNORMAL or s in TECH_EXCEPTIONS
# ======================================================

# 线程安全
print_lock = threading.Lock()
file_lock = threading.Lock()

# ---- Feishu 事件去重缓存 ----
EVENT_CACHE_TTL = int(os.getenv("FEISHU_EVENT_TTL_SECONDS", "600"))
_EVENT_CACHE = OrderedDict()
_event_lock = threading.Lock()

# 手动执行排队
MANUAL_PENDING = threading.Event()
_pending_lock = threading.Lock()
PENDING_CHAT_ID = None

# 执行互斥/状态
RUN_MUTEX = threading.Lock()
RUN_ACTIVE = threading.Event()      # 正在执行一轮
RUN_START_AT = 0.0                  # monotonic
RUN_SOURCE = ""                     # '手动执行' / '周期执行'
RUN_HOLDER = ""                     # "{source}::{thread-name}"
LOCK_STUCK_SINCE = 0.0              # 卡锁起始时间

# 人工中止当前轮
CANCEL_EVENT = threading.Event()

def _holder() -> str:
    return RUN_HOLDER or "<none>"

def _run_elapsed_sec() -> int:
    if not RUN_ACTIVE.is_set():
        return 0
    if RUN_START_AT <= 0:
        return 0  # 或者返回 -1 表示未知
    return int(time.monotonic() - RUN_START_AT)

def _lock_stuck_for() -> float:
    return 0.0 if LOCK_STUCK_SINCE == 0.0 else (time.monotonic() - LOCK_STUCK_SINCE)

def _try_unstick_mutex(force: bool = False) -> bool:
    """卡锁/脏锁自愈：支持非执行态和执行态两种场景；force=True 时无条件重置"""
    global RUN_MUTEX, LOCK_STUCK_SINCE
    now = time.monotonic()
    limit = CONFIG["FB"]["ROUND_TIMEOUT"] + CONFIG["FB"]["FUTURE_EXTRA_GRACE"]

    if force:
        print(f"[WATCHDOG] force reset RUN_MUTEX (holder={_holder()})")
        RUN_MUTEX = threading.Lock()
        RUN_ACTIVE.clear()
        globals()["RUN_SOURCE"] = ""
        globals()["RUN_HOLDER"] = ""
        CANCEL_EVENT.clear()
        LOCK_STUCK_SINCE = 0.0
        return True

    # 情况 A：锁被占用但不在执行态（原逻辑）
    if RUN_MUTEX.locked() and not RUN_ACTIVE.is_set():
        if LOCK_STUCK_SINCE == 0.0:
            LOCK_STUCK_SINCE = now
        if now - LOCK_STUCK_SINCE > limit:
            print(f"[WATCHDOG] reset RUN_MUTEX due to stale NON-ACTIVE lock "
                  f"(stuck_for={now-LOCK_STUCK_SINCE:.1f}s, holder={_holder()})")
            RUN_MUTEX = threading.Lock()
            globals()["RUN_SOURCE"] = ""
            globals()["RUN_HOLDER"] = ""
            LOCK_STUCK_SINCE = 0.0
            CANCEL_EVENT.clear()
            return True
        return False
    else:
        LOCK_STUCK_SINCE = 0.0

    # 情况 B：锁被占用且在执行态，但执行时长明显超时 → 直接强制复位
    if RUN_MUTEX.locked() and RUN_ACTIVE.is_set():
        start_at = RUN_START_AT or 0.0
        elapsed = now - start_at if start_at > 0 else now
        if elapsed > limit:
            print(f"[WATCHDOG] force reset RUN_MUTEX due to ACTIVE lock timeout "
                  f"(elapsed={elapsed:.1f}s > {limit}s, holder={_holder()})")
            RUN_MUTEX = threading.Lock()
            RUN_ACTIVE.clear()
            globals()["RUN_SOURCE"] = ""
            globals()["RUN_HOLDER"] = ""
            CANCEL_EVENT.clear()
            return True

    return False

# ---------------- Facebook Page 检测逻辑 ----------------
def now_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def now_local_str() -> str:
    tz = ZoneInfo(CONFIG["FEISHU"]["tz"])
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

def load_label_id_pairs(path: str) -> List[Tuple[str, str, str]]:
    """
    读取 pages.txt，支持两种格式：
    - 新格式：name-pageID-ownedBy   （name / ownedBy 都允许包含连字符 `-` 和空格）
    - 旧格式：name-pageID           （owner 将置为 '未知'）

    返回三元组列表：(label, page_id, owner)
    """
    if not os.path.exists(path):
        return []
    rows, seen = [], set()

    # 新格式：以中间“纯数字 pageID”作为锚点，左右都用 .+? 捕获，允许包含 '-'
    pat3 = re.compile(r"^\s*(.+?)-(\d{5,})-(.+?)\s*[,\s;，、]*$", re.UNICODE)

    # 旧格式（无 owner）：同样只要求中间是纯数字 pageID
    pat2 = re.compile(r"^\s*(.+?)-(\d{5,})\s*[,\s;，、]*$", re.UNICODE)

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue

            m3 = pat3.match(s)
            if m3:
                label = m3.group(1).strip()
                pid   = m3.group(2).strip()
                owner = m3.group(3).strip()
            else:
                m2 = pat2.match(s)
                if not m2:
                    continue
                label = m2.group(1).strip()
                pid   = m2.group(2).strip()
                owner = "未知"

            if pid not in seen:
                seen.add(pid)
                rows.append((label, pid, owner))
    return rows


def build_session() -> requests.Session:
    fb = CONFIG["FB"]
    sess = requests.Session()
    retries = Retry(
        total=fb["MAX_RETRIES"],
        backoff_factor=fb["BACKOFF_FACTOR"],
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=200, pool_maxsize=200)
    sess.mount("https://", adapter)
    sess.headers.update({"User-Agent": "fb-page-feishu-bot/1.1"})
    return sess

def classify_page_status(http_status: int, payload: Optional[Dict[str, Any]], had_token: bool, need_token_as_normal: bool) -> Tuple[str, int]:
    """返回 (status, normal_flag)；normal_flag: 1正常/0确定异常/-1未知/临时失败"""
    if http_status == 200 and isinstance(payload, dict) and payload.get("id"):
        return "OK", 1

    err = (payload or {}).get("error") if isinstance(payload, dict) else None
    code = (err or {}).get("code")
    msg = ((err or {}).get("message") or "").lower() if isinstance(err, dict) else ""

    if code == 803 or "unknown path components" in msg or "do not exist" in msg:
        return "NOT_FOUND", 0
    if code in (4, 17) or "limit" in msg:
        return "RATE_LIMIT", -1
    if code == 190 or "#10" in msg or "#200" in msg or "permission" in msg or "access token" in msg:
        if not had_token:
            return "NEED_TOKEN", (1 if need_token_as_normal else -1)
        return "AUTH_ERROR", -1

    if http_status is None:
        return "NETWORK_ERROR", -1

    return "UNKNOWN", -1

def probe_page(session: requests.Session, page_id: str, graph_version: str,
               access_token: Optional[str], timeout: int, probe_feed: bool) -> Dict[str, Any]:
    """
    读取 Page 基础字段；再读 /settings 拿年龄/国家墙。
    若用 System User Token 访问 /settings 返回 #200，则自动用 SU 换 Page Token 再试一次。
    """
    had_token = bool(access_token)
    base = f"https://graph.facebook.com/{graph_version}/{page_id}"

    # 1) 基础字段（注意已去掉 age_restrictions/country_restrictions）
    params = {"fields": "id,name,link,is_published,business{name}"}
    if had_token:
        params["access_token"] = access_token

    try:
        resp = session.get(base, params=params, timeout=timeout)
        try:
            data = resp.json()
        # 在 data = resp.json() 之后，加：
            owner_name = None
            if isinstance(data, dict):
                b = data.get("business")
                if isinstance(b, dict):
                    owner_name = b.get("name")
        except Exception:
            data = None

        # 2) 读 settings（AGE_/COUNTRY_），必要时回退用 Page Token
        age_rest = None
        country_rest = None
        settings_perm_err = False

        def _read_settings(token: Optional[str]) -> Tuple[Optional[str], Optional[str], int, Optional[str]]:
            sparams = {"fields": "setting,value"}
            if token:
                sparams["access_token"] = token
            r = session.get(f"{base}/settings", params=sparams, timeout=timeout)
            try:
                js = r.json()
            except Exception:
                js = None
            ar = cr = None
            if r.status_code == 200 and isinstance(js, dict):
                for it in (js.get("data") or []):
                    if it.get("setting") == "AGE_RESTRICTIONS":
                        ar = it.get("value")
                    elif it.get("setting") == "COUNTRY_RESTRICTIONS":
                        cr = it.get("value")
            emsg = (js or {}).get("error", {}).get("message") if isinstance(js, dict) else None
            return ar, cr, r.status_code, emsg

        if had_token:
            ar, cr, sc, em = _read_settings(access_token)
            if sc == 200:
                age_rest, country_rest = ar, cr
            elif em and "Insufficient administrative permission" in em:
                # 用 SU token 换 Page token 再读一次
                r2 = session.get(base, params={"fields": "access_token", "access_token": access_token}, timeout=timeout)
                js2 = r2.json() if "application/json" in (r2.headers.get("content-type") or "") else {}
                ptoken = (js2 or {}).get("access_token")
                if ptoken:
                    ar2, cr2, sc2, _ = _read_settings(ptoken)
                    if sc2 == 200:
                        age_rest, country_rest = ar2, cr2
                    else:
                        settings_perm_err = True
                else:
                    settings_perm_err = True

        # 3) 判定：未发布 / 国家墙 / 年龄墙 / OK
        status, normal_flag = classify_page_status(resp.status_code, data, had_token, CONFIG["FB"]["NEED_TOKEN_AS_NORMAL"])
        if resp.status_code == 200 and isinstance(data, dict) and data.get("id"):
            is_published = data.get("is_published")
            if is_published is False:
                status, normal_flag = "UNPUBLISHED", 0
            elif country_rest and str(country_rest).strip() not in ("", "None"):
                status, normal_flag = "RESTRICTED_COUNTRY", 0
            elif age_rest and str(age_rest).strip().lower() not in ("", "13+"):
                status, normal_flag = "RESTRICTED_AGE", 0
            else:
                status, normal_flag = "OK", 1
        else:
            is_published = None

        # 4) 可选：feed 探针侧写
        feed_hint = None
        if probe_feed and status in ("OK", "NEED_TOKEN", "AUTH_ERROR", "UNKNOWN", "RATE_LIMIT"):
            try:
                feed_params = {"limit": 1}
                if had_token:
                    feed_params["access_token"] = access_token
                rfeed = session.get(f"{base}/feed", params=feed_params, timeout=timeout)
                dfeed = rfeed.json() if "application/json" in (rfeed.headers.get("content-type") or "") else None
                if rfeed.status_code != 200:
                    er = (dfeed or {}).get("error") or {}
                    emsg2 = (er.get("message") or "").lower()
                    if any(k in emsg2 for k in ["permissions", "requires", "not authorized", "#10", "#200"]):
                        feed_hint = "FEED_PERMISSION_BLOCKED"
                    elif any(k in emsg2 for k in ["country", "age", "restricted"]):
                        feed_hint = "FEED_GEO_AGE_RESTRICTED"
            except Exception:
                pass
        if settings_perm_err and not feed_hint:
            feed_hint = "SETTINGS_PERMISSION_BLOCKED"

        return {
            "page_id": page_id,
            "http_status": resp.status_code,
            "status": status,
            "normal": normal_flag,
            "name": (data or {}).get("name") if isinstance(data, dict) else None,
            "owner_name": owner_name,  # ← 新增
            "link": (data or {}).get("link") if isinstance(data, dict) else None,
            "is_published": is_published,
            "age_restrictions": age_rest,
            "country_restrictions": country_rest,
            "checked_at": now_iso(),
            "feed_hint": feed_hint,
            "fb_error_code": (data or {}).get("error", {}).get("code") if isinstance(data, dict) else None,
            "fb_error_message": (data or {}).get("error", {}).get("message") if isinstance(data, dict) else None,
        }

    except requests.RequestException as e:
        return {
            "page_id": page_id, "http_status": None, "status": "NETWORK_ERROR", "normal": -1,
            "name": None, "owner_name": None, "link": None, "checked_at": now_iso(),
            "fb_error_code": None, "fb_error_message": str(e), "feed_hint": None,
        }
    

# ---- Owner 查询（仅对异常页调用）----
_owner_cache: dict[str, str] = {}

def fetch_owner_name(session: requests.Session, page_id: str, graph_version: str,
                     access_token: Optional[str], timeout: int) -> str:
    """
    只尝试轻量方式拿 Owner 名称；失败就返回 '未知'
    1) /{page-id}?fields=owner_business{name}
       - 若 BM 真正拥有该 Page，会返回 owner_business.name
    2) 兜底再试 /{page-id}?fields=owner_business （拿 id 再去 /{bm-id}?fields=name）
    3) 全部拿不到则 '未知'
    """
    if not access_token:
        return "未知"

    # 缓存命中
    if page_id in _owner_cache:
        return _owner_cache[page_id]

    base = f"https://graph.facebook.com/{graph_version}/{page_id}"
    try:
        # 方式 A：嵌套拿 name
        params = {"fields": "owner_business{name}", "access_token": access_token}
        r = session.get(base, params=params, timeout=timeout)
        j = r.json() if "application/json" in (r.headers.get("content-type") or "") else {}
        ob = (j or {}).get("owner_business") or {}
        name = (ob.get("name") or "").strip()
        if name:
            _owner_cache[page_id] = name
            return name

        # 方式 B：先拿 owner_business id 再查 BM 名
        params = {"fields": "owner_business", "access_token": access_token}
        r2 = session.get(base, params=params, timeout=timeout)
        j2 = r2.json() if "application/json" in (r2.headers.get("content-type") or "") else {}
        ob2 = (j2 or {}).get("owner_business") or {}
        bm_id = (ob2.get("id") or "").strip()
        if bm_id:
            r3 = session.get(f"https://graph.facebook.com/{graph_version}/{bm_id}",
                             params={"fields": "name", "access_token": access_token},
                             timeout=timeout)
            j3 = r3.json() if "application/json" in (r3.headers.get("content-type") or "") else {}
            nm = (j3 or {}).get("name")
            if nm:
                _owner_cache[page_id] = nm.strip()
                return _owner_cache[page_id]
    except Exception:
        pass

    _owner_cache[page_id] = "未知"
    return "未知"



def append_unique_lines(path: str, lines: List[str]):
    if not lines: return
    with file_lock:
        existed = set()
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                for ln in f:
                    s = ln.strip()
                    if s: existed.add(s)
        with open(path, "a", encoding="utf-8") as f:
            for s in lines:
                if s not in existed:
                    f.write(s + "\n")

def check_pages_one_round():
    fb = CONFIG["FB"]

    # 现在返回的是三元组 (label, page_id, owner)
    pairs = load_label_id_pairs(fb["MAPPING_FILE"])
    total = len(pairs)
    if total == 0:
        return 0, [], 0, []  # 无数据时四元组返回

    session = build_session()

    # 记录 page_id -> (label, owner)，方便合并结果
    id_to_meta = {pid: (label, owner) for (label, pid, owner) in pairs}
    results = []

    ex = ThreadPoolExecutor(max_workers=max(1, fb["CONCURRENCY"]))
    futs = {
        ex.submit(
            probe_page, session, page_id, fb["GRAPH_VERSION"],
            fb["ACCESS_TOKEN"], fb["REQUEST_TIMEOUT"], fb["PROBE_FEED"]
        ): page_id
        for (_, page_id, _) in pairs
    }



    try:
        deadline = time.monotonic() + int(fb.get("ROUND_TIMEOUT", 180))
        pending = set(futs.keys())
        done_idx = 0

        while pending:
            if CANCEL_EVENT.is_set():
                for fut in pending:
                    pid = futs[fut]
                    label, owner = id_to_meta.get(pid, ("", "未知"))
                    results.append({
                        "page_id": pid, "label": label, "owner_name": owner,
                        "status": "CANCELLED", "normal": -1, "checked_at": now_iso()
                    })
                    print(f"[{now_local_str()}] ⚠️ (cancel) {label}-{pid} -> CANCELLED")
                pending.clear()
                break

            if time.monotonic() >= deadline:
                for fut in pending:
                    pid = futs[fut]
                    label, owner = id_to_meta.get(pid, ("", "未知"))
                    results.append({
                        "page_id": pid, "label": label, "owner_name": owner,
                        "status": "TIMEOUT", "normal": -1, "checked_at": now_iso()
                    })
                    print(f"[{now_local_str()}] ⚠️ (timeout) {label}-{pid} -> TIMEOUT")
                pending.clear()
                break

            done, pending = wait(pending, timeout=1.0)
            for fut in done:
                pid = futs[fut]
                label, owner = id_to_meta.get(pid, ("", "未知"))
                try:
                    r = fut.result()
                except Exception as e:
                    r = {
                        "page_id": pid, "status": "WORKER_ERROR", "normal": -1,
                        "checked_at": now_iso(), "name": None,
                        "fb_error_message": str(e)
                    }

                # 合并：统一加上 label 和 owner（覆盖 probe_page 里可能返回的 business.name）
                r["label"] = label
                r["owner_name"] = owner
                results.append(r)

                done_idx += 1
                tag = "✅" if (r.get("status") or "").upper() == "OK" else (
                    "❌" if _bucket(r.get("status")) == "certain_abnormal" else "⚠️"
                )
                nm = f" | {r.get('name')}" if r.get("name") else ""
                extra = []
                if r.get("age_restrictions"): extra.append(f"age={r['age_restrictions']}")
                if r.get("country_restrictions"): extra.append(f"country={r['country_restrictions']}")
                suffix = f" [{', '.join(extra)}]" if extra else ""
                print(f"[{now_local_str()}] {tag} ({done_idx}/{total}) {label}-{r['page_id']}{nm} -> {r.get('status')}{suffix}")

    finally:
        ex.shutdown(wait=False, cancel_futures=True)
        # 注意：不再对异常页额外 fetch owner，此处可直接关掉 session
        try:
            session.close()
        except:
            pass

    

    def _pack_item(row: Dict[str, Any]) -> Dict[str, str]:
        page_name = (row.get("name") or "").strip() or (row.get("label") or "").strip() or "(未知名称)"
        owner = (row.get("owner_name") or "").strip() or "未知"
        status = (row.get("status") or "").upper()
        return {"name": page_name, "owner": owner, "status": status}



    # 三态写入文件：OK / UNPUBLISHED / NOT_FOUND
    # 另外：忽略类（年龄/国家墙）也按 OK 写入与计数
    tri_lines = []
    for r in results:
        st = (r.get("status") or "").upper()
        if not r.get("label"):
            continue
        bucket = _bucket(st)
        if bucket == "tri_state":
            tri_lines.append(f"{_fmt_label_id(r)} | {st}")
        elif bucket == "ignored":
            # 忽略类视为 OK
            tri_lines.append(f"{_fmt_label_id(r)} | OK")

    append_unique_lines(fb["OUT_TXT"], tri_lines)

    # OK 计数 = 真实 OK + 忽略类
    ok_count = sum(
        1 for r in results
        if ((r.get("status") or "").upper() == "OK") or (_bucket(r.get("status")) == "ignored")
    )


    # 确定异常（无 “- ” 前缀）
    certain_ab_items = [
        _pack_item(r) for r in results
        if r.get("label") and _bucket(r.get("status")) == "certain_abnormal"
    ]

    # 运行异常/权限类（有 “- ” 前缀由 push_summary 统一加，这里只给主体）

    tech_issues_items = [
        _pack_item(r) for r in results
        if r.get("label") and _bucket(r.get("status")) == "tech_exception"
    ]

    return ok_count, certain_ab_items, total, tech_issues_items

# ---------------- 飞书机器人（事件 + 发送） ----------------
app = Flask(__name__)
RUN_FLAG = threading.Event()
LAST_SUMMARY = {"time": None, "ok": 0, "ab": 0}
CURRENT_CHAT_ID = CONFIG["FEISHU"]["default_chat_id"] or None
_tenant_token_cache = {"token": None, "expire_at": 0}

def _feishu_base():
    return "https://open.feishu.cn" if CONFIG["FEISHU"]["domain"] == "feishu" else "https://open.larksuite.com"

def _get_tenant_access_token() -> Optional[str]:
    cache = _tenant_token_cache
    if cache["token"] and cache["expire_at"] > time.time() + 60:
        return cache["token"]
    url = _feishu_base() + "/open-apis/auth/v3/tenant_access_token/internal"
    resp = requests.post(url, json={"app_id": CONFIG["FEISHU"]["app_id"], "app_secret": CONFIG["FEISHU"]["app_secret"]}, timeout=8)
    data = resp.json()
    if data.get("code") == 0 and data.get("tenant_access_token"):
        cache["token"] = data["tenant_access_token"]
        cache["expire_at"] = time.time() + int(data.get("expire", 7200))
        return cache["token"]
    print("[Feishu] 获取 tenant_access_token 失败：", data)
    return None

def _send_text(chat_id: str, text: str):
    token = _get_tenant_access_token()
    if not token or not chat_id: return
    url = _feishu_base() + "/open-apis/im/v1/messages?receive_id_type=chat_id"
    maxlen = CONFIG["FEISHU"]["max_text_len"]
    chunks = [text[i:i+maxlen] for i in range(0, len(text), maxlen)] or [text]
    for chunk in chunks:
        body = {"receive_id": chat_id, "msg_type": "text", "content": json.dumps({"text": chunk}, ensure_ascii=False)}
        requests.post(url, headers={"Authorization": f"Bearer {token}"}, json=body, timeout=8)

@app.route("/", methods=["GET"])
def home():
    return "ok", 200

@app.route("/test_send", methods=["GET"])
def test_send():
    cid = request.args.get("cid") or CONFIG["FEISHU"]["default_chat_id"] or CURRENT_CHAT_ID
    if not cid:
        return "no chat_id; provide ?cid=oc_xxx or set FEISHU_DEFAULT_CHAT_ID", 400
    try:
        _send_text(cid, "测试：Page 机器人发消息 OK")
        return "sent", 200
    except Exception as e:
        return (f"send failed: {e}", 500)

def _target_chat_ids(preferred: Optional[str] = None):
    if preferred:
        return [preferred]
    ids_env = os.getenv("FEISHU_DEFAULT_CHAT_IDS", "")
    ids = [x.strip() for x in re.split(r"[,\s;]+", ids_env) if x.strip()]
    if not ids:
        one = os.getenv("FEISHU_DEFAULT_CHAT_ID") or CONFIG["FEISHU"].get("default_chat_id", "")
        if one: ids = [one]
    global CURRENT_CHAT_ID
    if not ids and CURRENT_CHAT_ID:
        ids = [CURRENT_CHAT_ID]
    uniq, seen = [], set()
    for x in ids:
        if x and x not in seen:
            uniq.append(x); seen.add(x)
    return uniq


def _group_by_owner_status(items: list[dict]) -> list[tuple[str, str, int]]:
    """
    将 [{name, owner, status}, ...] 按 (owner, status) 分组，返回排序后的 (owner, status, count) 列表。
    排序规则：count DESC，然后 owner ASC，然后 status ASC
    """
    from collections import defaultdict

    counter = defaultdict(int)
    for it in items:
        owner = (it.get("owner") or "未知").strip() or "未知"
        status = (it.get("status") or "UNKNOWN").strip().upper()
        counter[(owner, status)] += 1

    grouped = [ (owner, status, cnt) for (owner, status), cnt in counter.items() ]
    grouped.sort(key=lambda x: (-x[2], x[0].lower(), x[1]))  # 数量降序，再 owner/status 升序
    return grouped


#def push_summary(round_name, ok, ab_labels, chat_id=None,
#                 started_at: datetime | None = None,
#                 ended_at:   datetime | None = None,
#                 duration_sec: int | None = None,
#                 tech_issues: list[str] | None = None):
    

# 原：def push_summary(round_name, ok, ab_labels, ..., tech_issues: list[str] | None = None):
def push_summary(round_name, ok, ab_items, chat_id=None,
                 started_at: datetime | None = None,
                 ended_at:   datetime | None = None,
                 duration_sec: int | None = None,
                 tech_items: list[dict] | None = None):
    tech_items = tech_items or []
    targets = _target_chat_ids(chat_id)
    title = f"【FB Page 监控】{round_name}"

    lines = [title]
    if started_at and ended_at:
        lines += [
            f"开始：{started_at.strftime('%Y-%m-%d %H:%M:%S')}",
            f"结束：{ended_at.strftime('%Y-%m-%d %H:%M:%S')}（耗时{duration_sec or 0}s）",
        ]
        shown_time = ended_at.strftime('%Y-%m-%d %H:%M:%S')
    else:
        shown_time = now_local_str()
        lines.append(f"时间：{shown_time}")

    # 总体统计
    lines.append(f"正常(OK)：{ok}")

    # ====== 确定异常：按 (Owner, Status) 分组展示 ======
    if ab_items:
        lines.append(f"\n确定异常：{len(ab_items)}")  # 仍显示“页面条数”
        groups = _group_by_owner_status(ab_items)
        # 展示 Owner | Status | Count
        for owner, status, cnt in groups[:100]:
            owner_disp = owner or "未知"
            status_disp = status or "UNKNOWN"
            lines.append(f"{owner_disp} | {status_disp} | {cnt}")
        if len(groups) > 100:
            lines.append(f"... 还有 {len(groups)-100} 个分组")

    # ====== 运行异常/权限类：按 (Owner, Status) 分组展示 ======
    if tech_items:
        lines.append(f"\n运行异常/权限类：{len(tech_items)}")  # 仍显示“页面条数”
        groups2 = _group_by_owner_status(tech_items)
        for owner, status, cnt in groups2[:100]:
            owner_disp = owner or "未知"
            status_disp = status or "UNKNOWN"
            lines.append(f"{owner_disp} | {status_disp} | {cnt}")
        if len(groups2) > 100:
            lines.append(f"... 还有 {len(groups2)-100} 个分组")

    # 若两类都没有，按配置决定是否推送
    if not ab_items and not tech_items and not CONFIG["FEISHU"]["push_on_no_abnormal"]:
        return

    msg = "\n".join(lines)
    for tgt in targets:
        _send_text(tgt, msg)
    LAST_SUMMARY.update({"time": shown_time, "ok": ok, "ab": len(ab_items), "source": round_name})
    print(f"[PUSH] {round_name}: ok={ok} ab={len(ab_items)} tech={len(tech_items)} to={targets}")


def _drain_manual():
    while MANUAL_PENDING.is_set():
        MANUAL_PENDING.clear()
        with _pending_lock:
            cid = PENDING_CHAT_ID
        print(f"[PENDING] draining manual -> run_once chat={cid}")
        run_once_with_lock("手动执行", cid, notify_start=True)

def monitor_loop():
    while RUN_FLAG.is_set():
        run_once_with_lock("周期执行", None)
        _drain_manual()

        interval = int(CONFIG["SCHEDULE"]["interval_seconds"])
        for _ in range(interval):
            _try_unstick_mutex(force=False)
            if not RUN_FLAG.is_set() or MANUAL_PENDING.is_set() or CANCEL_EVENT.is_set():
                print(f"[MON] break wait: pause={not RUN_FLAG.is_set()} manual={MANUAL_PENDING.is_set()} cancel={CANCEL_EVENT.is_set()}")
                break
            time.sleep(1)

        if RUN_FLAG.is_set() and MANUAL_PENDING.is_set():
            _drain_manual()


def clean_text(s: str) -> str:
    if not s: return ""
    s = re.sub(r"<at[^>]*?>.*?</at>", "", s, flags=re.I | re.S)  # 去 <at>
    s = re.sub(r"@_user_\d+\s*", "", s)                          # 去 @_user_1
    s = s.replace("\u2005", " ").replace("\u200B", "")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _seen_event(event_id: str = None, message_id: str = None) -> bool:
    key = event_id or message_id
    if not key: return False
    now = time.time()
    with _event_lock:
        cutoff = now - EVENT_CACHE_TTL
        for k, ts in list(_EVENT_CACHE.items()):
            if ts < cutoff: _EVENT_CACHE.pop(k, None)
            else: break
        if key in _EVENT_CACHE: return True
        _EVENT_CACHE[key] = now
        if len(_EVENT_CACHE) > 2000:
            _EVENT_CACHE.popitem(last=False)
        return False

CMD_COOLDOWN_SEC = int(os.getenv("FEISHU_CMD_COOLDOWN_SECONDS", "8"))
_last_cmd_at = {}
_cmd_lock = threading.Lock()
def _cooldown_ok(chat_id: str, cmd: str) -> bool:
    now = time.time(); key = f"{chat_id}:{cmd}"
    with _cmd_lock:
        last = _last_cmd_at.get(key, 0)
        if now - last < CMD_COOLDOWN_SEC: return False
        _last_cmd_at[key] = now; return True

def run_once_with_lock(source, chat_id, notify_start=False):
    # 1) 试图获取锁；如卡锁则尝试自愈
    if not RUN_MUTEX.acquire(blocking=False):
        healed = _try_unstick_mutex(force=False)
        if healed and RUN_MUTEX.acquire(blocking=False):
            print("[WATCHDOG] lock was stale; recreated and re-acquired successfully")
        else:
            if chat_id:
                _send_text(chat_id, "上一轮仍在执行，本次已跳过")
            print(f"[RUN] skip overlapped: {source} | active={RUN_ACTIVE.is_set()} locked={RUN_MUTEX.locked()} holder={_holder()} stuck_for={_lock_stuck_for():.1f}s")
            return False

    globals()["RUN_HOLDER"] = f"{source}::{threading.current_thread().name}"

    try:
        # 2) 设置执行态 + 可选“已开始执行”
        RUN_ACTIVE.set()
        start_mono = time.monotonic()
        globals()["RUN_START_AT"] = start_mono  # ← 提前写
        globals()["RUN_SOURCE"] = source
        CANCEL_EVENT.clear()

        tz = ZoneInfo(CONFIG["FEISHU"]["tz"])
        start_wall = datetime.now(tz)
        print(f"[RUN] start {source}: at {start_wall.strftime('%Y-%m-%d %H:%M:%S')}")

        if notify_start and chat_id:
            _send_text(chat_id, "已开始执行，稍后回报结果")

        # 3) 一轮检测
        ok, ab_items, total, tech_items = check_pages_one_round()

        # 4) 结束并推送
        end_wall = datetime.now(tz)
        duration = max(0, int(time.monotonic() - start_mono))
        print(f"[RUN] end {source}: total={total} ok={ok} ab={len(ab_items)} tech={len(tech_items)} start={start_wall.strftime('%Y-%m-%d %H:%M:%S')} end={end_wall.strftime('%Y-%m-%d %H:%M:%S')} cost={duration}s")
        push_summary(source, ok, ab_items, chat_id=chat_id, started_at=start_wall, ended_at=end_wall, duration_sec=duration, tech_items=tech_items)
        return True

    except Exception as e:
        traceback.print_exc()
        if chat_id: _send_text(chat_id, f"{source}失败：{e}")
        return False

    finally:
        RUN_ACTIVE.clear()
        globals()["RUN_SOURCE"] = ""
        globals()["RUN_HOLDER"] = ""
        RUN_MUTEX.release()

# ==== 安全自启：支持模块导入后立即尝试一次 + 首个请求再确保一次 ====

_BOOT_ONCE = threading.Event()

def _start_monitor_if_needed():
    try:
        if os.getenv("BOT_AUTOSTART", "false").lower() == "true" and not RUN_FLAG.is_set():
            RUN_FLAG.set()
            threading.Thread(target=monitor_loop, daemon=True, name="monitor-loop").start()
            print(f"[BOOT] 自动轮询已启动（间隔 {CONFIG['SCHEDULE']['interval_seconds']}s）")
    except Exception as e:
        print("[BOOT] failed to start monitor:", e)

# 模块加载完成后，尝试自启一次（此时所有函数已定义，避免 NameError）
_start_monitor_if_needed()

# 保险：首个请求到来时再确保一次（Flask 3.x 用 before_request）
@app.before_request
def _ensure_boot_on_request():
    if not _BOOT_ONCE.is_set():
        _start_monitor_if_needed()
        _BOOT_ONCE.set()


@app.route("/feishu/events", methods=["POST"])
def feishu_events():
    data = request.get_json(force=True, silent=True) or {}
    print("[EVENT RAW]", json.dumps(data, ensure_ascii=False))

    # 可选：Verification Token
    vt = CONFIG["FEISHU"]["verification_token"]
    got = data.get("token") or data.get("header", {}).get("token")
    if vt and got != vt:
        print("[EVENT] verification token mismatch")
        return jsonify({"code": 1, "msg": "invalid token"}), 403

    # URL 校验
    if data.get("type") == "url_verification":
        return jsonify({"challenge": data.get("challenge")})

    # 解析
    event_id = (data.get("header") or {}).get("event_id")
    chat_id, message_id, text = None, None, ""
    content_raw, mentions, chat_type = "{}", [], "group"

    if data.get("schema") == "2.0" and (data.get("header") or {}).get("event_type") == "im.message.receive_v1":
        ev, msg = data.get("event", {}) or {}, (data.get("event", {}) or {}).get("message", {}) or {}
        message_id = msg.get("message_id"); chat_id = msg.get("chat_id")
        chat_type = msg.get("chat_type") or "group"; mentions = msg.get("mentions") or []
        content_raw = msg.get("content") or "{}"
        try: text = json.loads(content_raw).get("text", "")
        except Exception as e: print("[EVENT] parse content error:", e, content_raw)
    else:
        ev, msg = data.get("event", {}) or {}, (data.get("event", {}) or {}).get("message", {}) or {}
        event_id = event_id or ev.get("uuid") or msg.get("message_id")
        message_id = msg.get("message_id"); chat_id = msg.get("chat_id")
        chat_type = msg.get("chat_type") or "group"; mentions = msg.get("mentions") or []
        content = msg.get("content") or "{}"
        try: text = json.loads(content).get("text", "")
        except Exception as e: print("[EVENT] parse content error (legacy):", e, content)

    # 幂等去重
    if _seen_event(event_id=event_id, message_id=message_id):
        print(f"[DEDUP] drop event_id={event_id} message_id={message_id}")
        return jsonify({"code": 0})

    if not chat_id:
        print("[EVENT] no chat_id found, skip")
        return jsonify({"code": 0})

    # 仅群聊且@时响应；私聊不受限
    mentioned = bool(mentions)
    if not mentioned:
        try:
            mentioned = "@_user_" in (json.loads(content_raw).get("text", "") or "")
        except Exception:
            pass
    if chat_type == "group" and CONFIG["FEISHU"]["require_at"] and not mentioned:
        print("[EVENT] ignore group message without @mention")
        return jsonify({"code": 0})

    text = clean_text(text)
    def _norm_cmd(s: str) -> str:
        return re.sub(r"[。.!！]+$", "", s.strip())

    cmd, cmd_l = _norm_cmd(text), _norm_cmd(text).lower()
    print(f"[EVENT PARSED] chat_id={chat_id} text={repr(text)}")

    global CURRENT_CHAT_ID
    CURRENT_CHAT_ID = chat_id

    # === 指令 ===
    if cmd in ("chatid", "群id", "群ID"):
        _send_text(chat_id, f"chat_id: {chat_id}")
        print(f"[REPLY] chatid -> {chat_id}")

    elif cmd in ("执行", "立即执行") or cmd_l == "run":
        def _try_now():
            ok = run_once_with_lock("手动执行", chat_id, notify_start=True)
            if not ok:
                global PENDING_CHAT_ID
                with _pending_lock: PENDING_CHAT_ID = chat_id
                MANUAL_PENDING.set()
                print(f"[PENDING] queued manual for chat={chat_id}")
                _send_text(chat_id, "当前正在执行，本轮结束后将立即补跑一次")
        threading.Thread(target=_try_now, daemon=True).start()
        print("[REPLY] manual requested")

    elif cmd in ("开始", "start"):
        if not RUN_FLAG.is_set():
            RUN_FLAG.set()
            threading.Thread(target=monitor_loop, daemon=True).start()
            _send_text(chat_id, f"监控已启动（间隔 {CONFIG['SCHEDULE']['interval_seconds']}s）")
            print("[REPLY] monitoring started")
        else:
            _send_text(chat_id, "监控已在运行中")

    elif cmd in ("暂停", "停止", "stop"):
        RUN_FLAG.clear()
        _send_text(chat_id, "监控已暂停")
        print("[REPLY] monitoring paused")

    elif cmd in ("中止", "取消本轮", "abort", "cancel"):
        if RUN_ACTIVE.is_set():
            CANCEL_EVENT.set()
            _send_text(chat_id, "已请求中止当前轮，等待释放…")
            # 最多等 3 秒
            for _ in range(3):
                if not RUN_MUTEX.locked():
                    break
                time.sleep(1)
            if RUN_MUTEX.locked():
                _try_unstick_mutex(force=True)
                _send_text(chat_id, "未及时释放，已强制解锁")
            print("[REPLY] abort current round (with watchdog)")
        else:
            if RUN_MUTEX.locked():
                _try_unstick_mutex(force=True)
                _send_text(chat_id, "当前未在执行，但检测到锁异常，已强制解锁")
            else:
                _send_text(chat_id, "当前未在执行中，无需中止")

    elif cmd in ("抢占执行", "force", "force-run"):
        def _force():
            CANCEL_EVENT.set()
            _send_text(chat_id, "已请求中止当前轮，准备抢占执行…")
            for _ in range(5):
                if not RUN_MUTEX.locked(): break
                time.sleep(1)
            ok = run_once_with_lock("手动执行", chat_id, notify_start=True)
            if not ok:
                global PENDING_CHAT_ID
                with _pending_lock: PENDING_CHAT_ID = chat_id
                MANUAL_PENDING.set()
                _send_text(chat_id, "当前仍在释放中，已加入队列，稍后自动补跑")
        threading.Thread(target=_force, daemon=True).start()

    elif cmd in ("重置锁", "reset-lock", "unlock"):
        _try_unstick_mutex(force=True)
        _send_text(chat_id, f"已强制重置互斥锁（holder={_holder()}）")

    elif cmd in ("状态", "status"):
        try:
            running   = RUN_FLAG.is_set()
            executing = RUN_ACTIVE.is_set()
            locked    = RUN_MUTEX.locked()
            elapsed   = _run_elapsed_sec() if executing else 0
            src       = RUN_SOURCE or "无"
            holder    = _holder()
            lines = [
                f"运行：{running}",
                f"执行中：{executing}（来源：{src}，已耗时 {elapsed}s）",
                f"锁占用：{locked}（持有者：{holder}）",
                f"间隔：{CONFIG['SCHEDULE']['interval_seconds']}s",
                f"上次：{LAST_SUMMARY.get('time') or '无'}（OK={LAST_SUMMARY.get('ok',0)}, AB={LAST_SUMMARY.get('ab',0)}）",
            ]
            _send_text(chat_id, "\n".join(lines))
            print(f"[STATUS] running={running} executing={executing} locked={locked} holder={holder} elapsed={elapsed}s")
        except Exception as e:
            traceback.print_exc()
            _send_text(chat_id, f"状态查询失败：{e}")

    else:
        m = re.match(r"^(?:间隔|interval)\s*=\s*(\d+)$", cmd_l)
        if m:
            try:
                sec = int(m.group(1))
                CONFIG["SCHEDULE"]["interval_seconds"] = max(10, sec)
                _send_text(chat_id, f"已更新间隔为 {CONFIG['SCHEDULE']['interval_seconds']} 秒")
                print(f"[REPLY] interval -> {CONFIG['SCHEDULE']['interval_seconds']}")
            except Exception:
                _send_text(chat_id, "格式错误，示例：间隔=3600")
        else:
            print("[REPLY] ignore non-command")

    return jsonify({"code": 0})

@app.route("/healthz", methods=["GET"])
def healthz():
    return "ok page-bot", 200

def ensure_result_file():
    path = CONFIG["FB"]["OUT_TXT"]
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "a", encoding="utf-8"): pass

if __name__ == "__main__":
    ensure_result_file()
    app.run(host=CONFIG["SERVER"]["host"], port=CONFIG["SERVER"]["port"])

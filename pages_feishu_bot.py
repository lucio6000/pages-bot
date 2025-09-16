# -*- coding: utf-8 -*-
"""
Feishu 群机器人 + Facebook Page(粉丝页/主页) 监控（Render 版）
- 群里 @机器人：执行/开始/暂停/中止/抢占执行/间隔=3600/状态/chatid/帮助
- 支持：自动轮询 + 手动触发并存；去重；超时；可中止；本轮结束补跑手动
- 仅把“确定异常”的别名写入 result_pages.txt（追加去重）
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
        # 建议：System User 拿到的 Page 长效访问令牌。如果没有，也能做“存在/不存在”等有限检测
        "ACCESS_TOKEN": os.getenv("FB_ACCESS_TOKEN"),
        "NEED_TOKEN_AS_NORMAL": os.getenv("FB_NEED_TOKEN_AS_NORMAL", "false").lower() == "true",
        "OUT_TXT": os.getenv("FB_OUT_TXT", "result_pages.txt"),
        "REQUEST_TIMEOUT": int(os.getenv("FB_REQUEST_TIMEOUT", "6")),
        "MAX_RETRIES": int(os.getenv("FB_MAX_RETRIES", "1")),
        "BACKOFF_FACTOR": float(os.getenv("FB_BACKOFF_FACTOR", "0.5")),
        "ROUND_TIMEOUT": int(os.getenv("FB_ROUND_TIMEOUT", "180")),   # 整轮最多等待秒数
        "FUTURE_EXTRA_GRACE": int(os.getenv("FB_FUTURE_EXTRA_GRACE", "2")),
        # 是否额外探针访问 feed（用来侧写年龄/国家墙），没 token 时建议打开；有 token 也可保守关闭
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
    return int(time.monotonic() - RUN_START_AT) if RUN_ACTIVE.is_set() else 0

def _lock_stuck_for() -> float:
    return 0.0 if LOCK_STUCK_SINCE == 0.0 else (time.monotonic() - LOCK_STUCK_SINCE)

def _try_unstick_mutex(force: bool = False) -> bool:
    """卡锁/脏锁自愈：在锁被占但非执行态且超时后重建锁；force=True时无条件重置"""
    global RUN_MUTEX, LOCK_STUCK_SINCE
    if force:
        print(f"[WATCHDOG] force reset RUN_MUTEX (holder={_holder()})")
        RUN_MUTEX = threading.Lock()
        RUN_ACTIVE.clear()
        _set_src("")
        _set_holder("")
        CANCEL_EVENT.clear()
        LOCK_STUCK_SINCE = 0.0
        return True

    if RUN_MUTEX.locked() and not RUN_ACTIVE.is_set():
        now = time.monotonic()
        if LOCK_STUCK_SINCE == 0.0:
            LOCK_STUCK_SINCE = now
        limit = CONFIG["FB"]["ROUND_TIMEOUT"] + CONFIG["FB"]["FUTURE_EXTRA_GRACE"]
        if now - LOCK_STUCK_SINCE > limit:
            print(f"[WATCHDOG] reset RUN_MUTEX due to stale lock (stuck_for={now-LOCK_STUCK_SINCE:.1f}s, holder={_holder()})")
            RUN_MUTEX = threading.Lock()
            _set_src(""); _set_holder("")
            LOCK_STUCK_SINCE = 0.0
            CANCEL_EVENT.clear()
            return True
    else:
        LOCK_STUCK_SINCE = 0.0
    return False

def _set_holder(v: str):
    globals()["RUN_HOLDER"] = v

def _set_src(v: str):
    globals()["RUN_SOURCE"] = v

# ---------------- Facebook Page 检测逻辑 ----------------
def now_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def now_local_str() -> str:
    tz = ZoneInfo(CONFIG["FEISHU"]["tz"])
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

def load_label_id_pairs(path: str) -> List[Tuple[str, str]]:
    if not os.path.exists(path):
        return []
    pairs, seen = [], set()
    pat = re.compile(r"([^\s]+?)-(\d{5,})")
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"): continue
            m = pat.search(s)
            if not m: continue
            label, pid = m.group(1).strip().rstrip(",;，、"), m.group(2).strip().rstrip(",;，、")
            if pid not in seen:
                seen.add(pid); pairs.append((label, pid))
    return pairs

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
    sess.headers.update({"User-Agent": "fb-page-feishu-bot/1.0"})
    return sess

def classify_page_status(http_status: int, payload: Optional[Dict[str, Any]], had_token: bool, need_token_as_normal: bool) -> Tuple[str, int]:
    """返回 (status, normal_flag)；normal_flag: 1正常/0确定异常/-1未知/临时失败"""
    if http_status == 200 and isinstance(payload, dict) and payload.get("id"):
        # 拿到基础信息，先判断字段
        is_published = payload.get("is_published")
        age_rest = payload.get("age_restrictions")
        country_rest = payload.get("country_restrictions")
        if is_published is False:
            return "UNPUBLISHED", 0
        if age_rest and str(age_rest).strip().lower() not in ("", "13+"):
            return "RESTRICTED_AGE", 0
        if country_rest and str(country_rest).strip() not in ("", "None"):
            return "RESTRICTED_COUNTRY", 0
        return "OK", 1

    err = (payload or {}).get("error") if isinstance(payload, dict) else None
    code = (err or {}).get("code")
    msg = ((err or {}).get("message") or "").lower()

    if code == 803 or "unknown path components" in msg or "do not exist" in msg:
        return "NOT_FOUND", 0
    if code in (4, 17) or "limit" in msg:
        return "RATE_LIMIT", -1
    if code == 190 or "#10" in msg or "#200" in msg or "permission" in msg or "access token" in msg:
        # 没 token 或权限不足 → “未知/无法确认”，不计入确定异常
        if not had_token:
            # 可根据 need_token_as_normal 决定是否把 NEED_TOKEN 视为正常
            return "NEED_TOKEN", (1 if need_token_as_normal else -1)
        return "AUTH_ERROR", -1

    return "UNKNOWN", -1

def probe_page(session: requests.Session, page_id: str, graph_version: str, access_token: Optional[str], timeout: int, probe_feed: bool) -> Dict[str, Any]:
    had_token = bool(access_token)
    base = f"https://graph.facebook.com/{graph_version}/{page_id}"
    params = {
        "fields": "id,name,link,is_published,verification_status,fan_count,age_restrictions,country_restrictions,is_unclaimed,is_permanently_closed"
    }
    if had_token: params["access_token"] = access_token

    try:
        resp = session.get(base, params=params, timeout=timeout)
        data = None
        try:
            data = resp.json()
        except Exception:
            pass

        status, normal_flag = classify_page_status(resp.status_code, data, had_token, CONFIG["FB"]["NEED_TOKEN_AS_NORMAL"])

        # 可选：feed 探针做侧写（仅在尚未确定异常时再探）
        feed_hint = None
        if probe_feed and status in ("OK", "NEED_TOKEN", "AUTH_ERROR", "UNKNOWN", "RATE_LIMIT"):
            try:
                feed_params = {"limit": 1}
                if had_token: feed_params["access_token"] = access_token
                r2 = session.get(f"{base}/feed", params=feed_params, timeout=timeout)
                d2 = r2.json() if "application/json" in (r2.headers.get("content-type") or "") else None
                if r2.status_code != 200:
                    er = (d2 or {}).get("error") or {}
                    emsg = (er.get("message") or "").lower()
                    if any(k in emsg for k in ["permissions", "requires", "not authorized", "#10", "#200"]):
                        feed_hint = "FEED_PERMISSION_BLOCKED"
                    elif any(k in emsg for k in ["country", "age", "restricted"]):
                        feed_hint = "FEED_GEO_AGE_RESTRICTED"
            except Exception:
                pass

        return {
            "page_id": page_id,
            "http_status": resp.status_code,
            "status": status,
            "normal": normal_flag,
            "name": (data or {}).get("name") if isinstance(data, dict) else None,
            "link": (data or {}).get("link") if isinstance(data, dict) else None,
            "is_published": (data or {}).get("is_published") if isinstance(data, dict) else None,
            "age_restrictions": (data or {}).get("age_restrictions") if isinstance(data, dict) else None,
            "country_restrictions": (data or {}).get("country_restrictions") if isinstance(data, dict) else None,
            "checked_at": now_iso(),
            "feed_hint": feed_hint,
            "fb_error_code": (data or {}).get("error", {}).get("code") if isinstance(data, dict) else None,
            "fb_error_message": (data or {}).get("error", {}).get("message") if isinstance(data, dict) else None,
        }

    except requests.RequestException as e:
        return {
            "page_id": page_id, "http_status": None, "status": "NETWORK_ERROR", "normal": -1,
            "name": None, "link": None, "checked_at": now_iso(),
            "fb_error_code": None, "fb_error_message": str(e), "feed_hint": None,
        }

def is_abnormal(row: Dict[str, Any]) -> bool:
    # 只把“确定异常”入清单
    return row.get("status") in {"UNPUBLISHED", "RESTRICTED_AGE", "RESTRICTED_COUNTRY", "NOT_FOUND"}

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
    pairs = load_label_id_pairs(fb["MAPPING_FILE"])
    total = len(pairs)
    if total == 0:
        return 0, [], 0

    session = build_session()
    id_to_label = {pid: label for (label, pid) in pairs}
    results = []

    ex = ThreadPoolExecutor(max_workers=max(1, fb["CONCURRENCY"]))
    futs = {
        ex.submit(
            probe_page, session, page_id, fb["GRAPH_VERSION"],
            fb["ACCESS_TOKEN"], fb["REQUEST_TIMEOUT"], fb["PROBE_FEED"]
        ): page_id
        for _, page_id in pairs
    }

    try:
        deadline = time.monotonic() + int(fb.get("ROUND_TIMEOUT", 180))
        pending = set(futs.keys())
        done_idx = 0

        while pending:
            if CANCEL_EVENT.is_set():
                for fut in pending:
                    pid = futs[fut]; label = id_to_label.get(pid, "")
                    results.append({"page_id": pid, "label": label, "status": "CANCELLED", "normal": -1, "checked_at": now_iso()})
                    print(f"[{now_local_str()}] ⚠️ (cancel) {label}-{pid} -> CANCELLED")
                pending.clear()
                break

            if time.monotonic() >= deadline:
                for fut in pending:
                    pid = futs[fut]; label = id_to_label.get(pid, "")
                    results.append({"page_id": pid, "label": label, "status": "TIMEOUT", "normal": -1, "checked_at": now_iso()})
                    print(f"[{now_local_str()}] ⚠️ (timeout) {label}-{pid} -> TIMEOUT")
                pending.clear()
                break

            done, pending = wait(pending, timeout=1.0)
            for fut in done:
                pid = futs[fut]; label = id_to_label.get(pid, "")
                try:
                    r = fut.result()
                except Exception as e:
                    r = {"page_id": pid, "status": "WORKER_ERROR", "normal": -1, "checked_at": now_iso(), "name": None}
                r["label"] = label; results.append(r)
                done_idx += 1
                tag = "✅" if r.get("normal") == 1 else ("❌" if r.get("normal") == 0 else "⚠️")
                nm = f" | {r.get('name')}" if r.get("name") else ""
                print(f"[{now_local_str()}] {tag} ({done_idx}/{total}) {label}-{r['page_id']}{nm} -> {r.get('status')}")

    finally:
        ex.shutdown(wait=False, cancel_futures=True)
        try: session.close()
        except: pass

    abnormal_labels = [f"{r['label']}异常" for r in results if is_abnormal(r) and r.get("label")]
    append_unique_lines(fb["OUT_TXT"], abnormal_labels)
    ok_count = sum(1 for r in results if r.get("normal") == 1)
    return ok_count, abnormal_labels, total

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

def push_summary(round_name, ok, ab_labels, chat_id=None,
                 started_at: datetime | None = None,
                 ended_at:   datetime | None = None,
                 duration_sec: int | None = None):
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
    lines.append(f"正常：{ok}")
    if ab_labels:
        lines.append(f"异常：{len(ab_labels)}")
        lines.append("异常清单（最多50条）：\n" + "\n".join(f"- {x}" for x in ab_labels[:50]))
        if len(ab_labels) > 50:
            lines.append(f"... 还有 {len(ab_labels)-50} 条")
    else:
        if not CONFIG["FEISHU"]["push_on_no_abnormal"]:
            return
        lines.append("本轮无确定异常 ✅")
    msg = "\n".join(lines)
    for tgt in targets:
        _send_text(tgt, msg)
    LAST_SUMMARY.update({"time": shown_time, "ok": ok, "ab": len(ab_labels), "source": round_name})
    print(f"[PUSH] {round_name}: ok={ok} ab={len(ab_labels)} to={targets}")

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
            # 等待期顺便跑一次非强制自愈（卡锁但非执行态 & 超时）
            _try_unstick_mutex(force=False)
            if not RUN_FLAG.is_set() or MANUAL_PENDING.is_set() or CANCEL_EVENT.is_set():
                print(f"[MON] break wait: pause={not RUN_FLAG.is_set()} manual={MANUAL_PENDING.is_set()} cancel={CANCEL_EVENT.is_set()}")
                break
            time.sleep(1)

        if RUN_FLAG.is_set() and MANUAL_PENDING.is_set():
            _drain_manual()

# ==== 开机自启内部轮询（可选）====
if os.getenv("BOT_AUTOSTART", "false").lower() == "true":
    if not RUN_FLAG.is_set():
        RUN_FLAG.set()
        threading.Thread(target=monitor_loop, daemon=True).start()
        print(f"[BOOT] 自动轮询已启动（间隔 {CONFIG['SCHEDULE']['interval_seconds']}s）")

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

    _set_holder(f"{source}::{threading.current_thread().name}")

    try:
        # 2) 设置执行态 + 可选“已开始执行”
        RUN_ACTIVE.set()
        tz = ZoneInfo(CONFIG["FEISHU"]["tz"])
        start_wall = datetime.now(tz)
        start_mono = time.monotonic()
        globals()["RUN_START_AT"] = start_mono
        _set_src(source)
        CANCEL_EVENT.clear()

        print(f"[RUN] start {source}: at {start_wall.strftime('%Y-%m-%d %H:%M:%S')}")
        if notify_start and chat_id:
            _send_text(chat_id, "已开始执行，稍后回报结果")

        # 3) 一轮检测
        ok, ab_labels, total = check_pages_one_round()

        # 4) 结束并推送
        end_wall = datetime.now(tz)
        duration = max(0, int(time.monotonic() - start_mono))
        print(f"[RUN] end {source}: total={total} ok={ok} ab={len(ab_labels)} start={start_wall.strftime('%Y-%m-%d %H:%M:%S')} end={end_wall.strftime('%Y-%m-%d %H:%M:%S')} cost={duration}s")
        push_summary(source, ok, ab_labels, chat_id=chat_id, started_at=start_wall, ended_at=end_wall, duration_sec=duration)
        return True

    except Exception as e:
        traceback.print_exc()
        if chat_id: _send_text(chat_id, f"{source}失败：{e}")
        return False

    finally:
        RUN_ACTIVE.clear()
        _set_src(""); _set_holder("")
        RUN_MUTEX.release()

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
            _send_text(chat_id, "已请求中止当前轮，1-2 秒内释放执行状态")
            print("[REPLY] abort current round")
        else:
            if RUN_MUTEX.locked():
                _try_unstick_mutex(force=True)
                _send_text(chat_id, "检测到锁异常，已强制解锁")
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

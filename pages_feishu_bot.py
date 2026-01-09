# -*- coding: utf-8 -*-
"""
Feishu 群机器人 + Facebook Page(粉丝页/主页) 监控（Render 版，多租户）
- 事件入口：/feishu/<tenant>/events
- 配置来源：TENANTS_JSON（必须，且键名统一大写）
- 群里 @机器人：执行/开始/暂停/中止/抢占执行/间隔=3600/状态/chatid/帮助
- 支持：自动轮询 + 手动触发并存；去重；超时；可中止；本轮结束补跑手动
- 写 result 文件：仅三态 OK / UNPUBLISHED / NOT_FOUND（年龄墙/国家墙按 OK 写入）
- “运行异常/权限类”：NEED_TOKEN/AUTH_ERROR/RATE_LIMIT/UNKNOWN/NETWORK_ERROR/TIMEOUT/CANCELLED/WORKER_ERROR
  在飞书推送中单独分区展示
- 推送分组：按 (Owner, Status) 分组，每组输出 owner|status|count + page_id 列表

✅ 重点修复（你之前踩过的坑）：
1) 配置键统一大写：APP_ID/APP_SECRET/VERIFICATION_TOKEN/DEFAULT_CHAT_ID 等
2) @ 机器人文本解析：清理 <at> / @_user_x，避免“未知指令”
3) 多租户隔离：每个 tenant 独立锁、事件去重缓存、token cache、轮询线程、状态
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

# ==========================================================
# Flask
# ==========================================================
APP = Flask(__name__)

# ==========================================================
# TENANTS_JSON（必须）
# ==========================================================
TENANTS_ENV = os.getenv("TENANTS_JSON")
if not TENANTS_ENV:
    raise RuntimeError("TENANTS_JSON env var is required")

try:
    TENANTS: Dict[str, Any] = json.loads(TENANTS_ENV)
except Exception as e:
    raise RuntimeError(f"TENANTS_JSON parse error: {e}")

if not isinstance(TENANTS, dict) or not TENANTS:
    raise RuntimeError("TENANTS_JSON must be a non-empty JSON object (dict)")

# ==========================================================
# 全局常量（与你原逻辑一致）
# ==========================================================
CERTAIN_ABNORMAL = {"UNPUBLISHED", "NOT_FOUND"}
TECH_EXCEPTIONS  = {"NEED_TOKEN", "AUTH_ERROR", "RATE_LIMIT", "UNKNOWN",
                    "NETWORK_ERROR", "TIMEOUT", "CANCELLED", "WORKER_ERROR"}
IGNORED_STATUSES = {"RESTRICTED_AGE", "RESTRICTED_COUNTRY"}
RESULT_KEEP_STATUSES = {"OK", "UNPUBLISHED", "NOT_FOUND"}

# 线程安全：写文件
file_lock = threading.Lock()

# ==========================================================
# 工具函数
# ==========================================================
def now_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def now_local_str(tz_name: str) -> str:
    tz = ZoneInfo(tz_name)
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

def _bucket(status: str) -> str:
    s = (status or "").upper()
    if s in CERTAIN_ABNORMAL:      return "certain_abnormal"
    if s in TECH_EXCEPTIONS:       return "tech_exception"
    if s in IGNORED_STATUSES:      return "ignored"
    if s in RESULT_KEEP_STATUSES:  return "tri_state"
    return "other"

def _fmt_label_id(r: Dict[str, Any]) -> str:
    label = (r.get("label") or "").strip()
    rid = r.get("page_id") or r.get("app_id") or ""
    return f"{label}-{rid}".strip("-")

AT_RE = re.compile(r"<at[^>]*?>.*?</at>", re.I | re.S)

def clean_text(s: str) -> str:
    """✅ 修复 @ 机器人导致的未知指令：清理 <at> 和 @_user_x"""
    if not s:
        return ""
    s = AT_RE.sub("", s)
    s = re.sub(r"@_user_\d+\s*", "", s)
    s = s.replace("\u2005", " ").replace("\u200B", "")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _norm_cmd(s: str) -> str:
    return re.sub(r"[。.!！]+$", "", (s or "").strip())

def _cfg_required(d: dict, key: str, err: str):
    if key not in d or d[key] in (None, ""):
        raise RuntimeError(err)
    return d[key]

def _get_tenant_cfg(tenant: str) -> dict:
    if tenant not in TENANTS:
        raise KeyError(f"unknown tenant: {tenant}")
    cfg = TENANTS[tenant]
    if not isinstance(cfg, dict):
        raise RuntimeError(f"{tenant} config must be an object")
    return cfg

def _feishu_base(domain: str) -> str:
    return "https://open.feishu.cn" if (domain or "feishu") == "feishu" else "https://open.larksuite.com"

# ==========================================================
# 配置校验（大写键）
# ==========================================================
def _validate_tenants():
    for tenant, cfg in TENANTS.items():
        if not isinstance(cfg, dict):
            raise RuntimeError(f"{tenant} config must be an object")

        feishu = cfg.get("FEISHU")
        fb = cfg.get("FB")

        if not isinstance(feishu, dict):
            raise RuntimeError(f"{tenant} missing FEISHU config")

        # 飞书必须项（大写）
        _cfg_required(feishu, "APP_ID", f"{tenant}.FEISHU missing APP_ID")
        _cfg_required(feishu, "APP_SECRET", f"{tenant}.FEISHU missing APP_SECRET")
        _cfg_required(feishu, "VERIFICATION_TOKEN", f"{tenant}.FEISHU missing VERIFICATION_TOKEN")
        # DEFAULT_CHAT_ID 可选，但建议配置
        if "DEFAULT_CHAT_ID" in feishu and feishu["DEFAULT_CHAT_ID"] in (None, ""):
            raise RuntimeError(f"{tenant}.FEISHU DEFAULT_CHAT_ID is empty")

        # Facebook 配置（允许缺 ACCESS_TOKEN，但会分类 NEED_TOKEN）
        if not isinstance(fb, dict):
            raise RuntimeError(f"{tenant} missing FB config")

        # FB 必要项
        _cfg_required(fb, "MAPPING_FILE", f"{tenant}.FB missing MAPPING_FILE")
        _cfg_required(fb, "OUT_TXT", f"{tenant}.FB missing OUT_TXT")

        # 兜底默认
        feishu.setdefault("TZ", os.getenv("BOT_TZ", "Asia/Shanghai"))
        feishu.setdefault("REQUIRE_AT", os.getenv("FEISHU_REQUIRE_AT", "true").lower() == "true")
        feishu.setdefault("DOMAIN", os.getenv("FEISHU_DOMAIN", "feishu"))
        feishu.setdefault("PUSH_ON_NO_ABNORMAL", os.getenv("FEISHU_PUSH_ON_NO_ABNORMAL", "false").lower() == "true")
        feishu.setdefault("MAX_TEXT_LEN", int(os.getenv("FEISHU_MAX_TEXT_LEN", "1800")))
        feishu.setdefault("MAX_IDS_PER_GROUP", int(os.getenv("FEISHU_MAX_IDS_PER_GROUP", "200")))
        feishu.setdefault("MAX_GROUPS", int(os.getenv("FEISHU_MAX_GROUPS", "200")))

        fb.setdefault("CONCURRENCY", int(os.getenv("FB_CONCURRENCY", "6")))
        fb.setdefault("GRAPH_VERSION", os.getenv("FB_GRAPH_VERSION", "v21.0"))
        fb.setdefault("ACCESS_TOKEN", os.getenv("FB_ACCESS_TOKEN"))
        fb.setdefault("NEED_TOKEN_AS_NORMAL", os.getenv("FB_NEED_TOKEN_AS_NORMAL", "false").lower() == "true")
        fb.setdefault("REQUEST_TIMEOUT", int(os.getenv("FB_REQUEST_TIMEOUT", "6")))
        fb.setdefault("MAX_RETRIES", int(os.getenv("FB_MAX_RETRIES", "1")))
        fb.setdefault("BACKOFF_FACTOR", float(os.getenv("FB_BACKOFF_FACTOR", "0.5")))
        fb.setdefault("ROUND_TIMEOUT", int(os.getenv("FB_ROUND_TIMEOUT", "180")))
        fb.setdefault("FUTURE_EXTRA_GRACE", int(os.getenv("FB_FUTURE_EXTRA_GRACE", "2")))
        fb.setdefault("PROBE_FEED", os.getenv("FB_PROBE_FEED", "true").lower() == "true")

        sched = cfg.setdefault("SCHEDULE", {})
        if not isinstance(sched, dict):
            raise RuntimeError(f"{tenant}.SCHEDULE must be an object")
        sched.setdefault("INTERVAL_SECONDS", int(os.getenv("BOT_INTERVAL_SECONDS", "3600")))

_validate_tenants()

# ==========================================================
# Tenant 独立状态（锁/去重/token cache/线程）
# ==========================================================
EVENT_CACHE_TTL = int(os.getenv("FEISHU_EVENT_TTL_SECONDS", "600"))

TENANT_STATE: Dict[str, Dict[str, Any]] = {}

def _init_state():
    for tenant in TENANTS.keys():
        TENANT_STATE[tenant] = {
            # 去重缓存
            "EVENT_CACHE": OrderedDict(),
            "EVENT_LOCK": threading.Lock(),

            # 手动执行排队
            "MANUAL_PENDING": threading.Event(),
            "PENDING_LOCK": threading.Lock(),
            "PENDING_CHAT_ID": None,

            # 执行互斥/状态
            "RUN_MUTEX": threading.Lock(),
            "RUN_ACTIVE": threading.Event(),
            "RUN_START_AT": 0.0,
            "RUN_SOURCE": "",
            "RUN_HOLDER": "",
            "LOCK_STUCK_SINCE": 0.0,
            "CANCEL_EVENT": threading.Event(),

            # 轮询开关
            "RUN_FLAG": threading.Event(),

            # 总结
            "LAST_SUMMARY": {"time": None, "ok": 0, "ab": 0, "source": ""},

            # 当前 chat id（动态更新）
            "CURRENT_CHAT_ID": _get_tenant_cfg(tenant)["FEISHU"].get("DEFAULT_CHAT_ID") or None,

            # tenant_access_token 缓存
            "TENANT_TOKEN_CACHE": {"token": None, "expire_at": 0},

            # monitor 线程只启动一次
            "MONITOR_THREAD_STARTED": False,
        }

_init_state()

def _st(tenant: str) -> Dict[str, Any]:
    return TENANT_STATE[tenant]

def _holder(tenant: str) -> str:
    return _st(tenant)["RUN_HOLDER"] or "<none>"

def _run_elapsed_sec(tenant: str) -> int:
    st = _st(tenant)
    if not st["RUN_ACTIVE"].is_set() or st["RUN_START_AT"] <= 0:
        return 0
    return int(time.monotonic() - st["RUN_START_AT"])

def _lock_stuck_for(tenant: str) -> float:
    st = _st(tenant)
    return 0.0 if st["LOCK_STUCK_SINCE"] == 0.0 else (time.monotonic() - st["LOCK_STUCK_SINCE"])

def _try_unstick_mutex(tenant: str, force: bool = False) -> bool:
    st = _st(tenant)
    fb = _get_tenant_cfg(tenant)["FB"]
    now = time.monotonic()
    limit = int(fb["ROUND_TIMEOUT"]) + int(fb["FUTURE_EXTRA_GRACE"])

    if force:
        print(f"[WATCHDOG] tenant={tenant} force reset RUN_MUTEX (holder={_holder(tenant)})")
        st["RUN_MUTEX"] = threading.Lock()
        st["RUN_ACTIVE"].clear()
        st["RUN_SOURCE"] = ""
        st["RUN_HOLDER"] = ""
        st["CANCEL_EVENT"].clear()
        st["LOCK_STUCK_SINCE"] = 0.0
        return True

    if st["RUN_MUTEX"].locked() and not st["RUN_ACTIVE"].is_set():
        if st["LOCK_STUCK_SINCE"] == 0.0:
            st["LOCK_STUCK_SINCE"] = now
        if now - st["LOCK_STUCK_SINCE"] > limit:
            print(f"[WATCHDOG] tenant={tenant} reset RUN_MUTEX due to stale NON-ACTIVE lock "
                  f"(stuck_for={now-st['LOCK_STUCK_SINCE']:.1f}s, holder={_holder(tenant)})")
            st["RUN_MUTEX"] = threading.Lock()
            st["RUN_SOURCE"] = ""
            st["RUN_HOLDER"] = ""
            st["LOCK_STUCK_SINCE"] = 0.0
            st["CANCEL_EVENT"].clear()
            return True
        return False
    else:
        st["LOCK_STUCK_SINCE"] = 0.0

    if st["RUN_MUTEX"].locked() and st["RUN_ACTIVE"].is_set():
        start_at = st["RUN_START_AT"] or 0.0
        elapsed = now - start_at if start_at > 0 else now
        if elapsed > limit:
            print(f"[WATCHDOG] tenant={tenant} force reset RUN_MUTEX due to ACTIVE lock timeout "
                  f"(elapsed={elapsed:.1f}s > {limit}s, holder={_holder(tenant)})")
            st["RUN_MUTEX"] = threading.Lock()
            st["RUN_ACTIVE"].clear()
            st["RUN_SOURCE"] = ""
            st["RUN_HOLDER"] = ""
            st["CANCEL_EVENT"].clear()
            return True

    return False

# ==========================================================
# Facebook Page 检测逻辑（保留你原逻辑，改为 tenant 参数）
# ==========================================================
def load_label_id_pairs(path: str) -> List[Tuple[str, str, str]]:
    """
    读取 pages.txt，支持：
    - 新格式：name-pageID-ownedBy
    - 旧格式：name-pageID（owner='未知'）
    返回：(label, page_id, owner)
    """
    if not os.path.exists(path):
        return []
    rows, seen = [], set()
    pat3 = re.compile(r"^\s*(.+?)-(\d{5,})-(.+?)\s*[,\s;，、]*$", re.UNICODE)
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

def build_session(tenant: str) -> requests.Session:
    fb = _get_tenant_cfg(tenant)["FB"]
    sess = requests.Session()
    retries = Retry(
        total=int(fb["MAX_RETRIES"]),
        backoff_factor=float(fb["BACKOFF_FACTOR"]),
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=200, pool_maxsize=200)
    sess.mount("https://", adapter)
    sess.headers.update({"User-Agent": f"fb-page-feishu-bot/tenant-{tenant}/1.3"})
    return sess

def classify_page_status(http_status: int, payload: Optional[Dict[str, Any]],
                         had_token: bool, need_token_as_normal: bool) -> Tuple[str, int]:
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

def probe_page(tenant: str, session: requests.Session, page_id: str) -> Dict[str, Any]:
    fb = _get_tenant_cfg(tenant)["FB"]

    graph_version = fb["GRAPH_VERSION"]
    access_token = fb.get("ACCESS_TOKEN")
    timeout = int(fb["REQUEST_TIMEOUT"])
    probe_feed = bool(fb["PROBE_FEED"])

    had_token = bool(access_token)
    base = f"https://graph.facebook.com/{graph_version}/{page_id}"
    params = {"fields": "id,name,link,is_published,business{name}"}
    if had_token:
        params["access_token"] = access_token

    try:
        resp = session.get(base, params=params, timeout=timeout)
        try:
            data = resp.json()
        except Exception:
            data = None

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

        status, normal_flag = classify_page_status(resp.status_code, data, had_token, bool(fb["NEED_TOKEN_AS_NORMAL"]))
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
            "name": None, "link": None, "checked_at": now_iso(),
            "fb_error_code": None, "fb_error_message": str(e), "feed_hint": None,
        }

def append_unique_lines(path: str, lines: List[str]):
    if not lines:
        return
    with file_lock:
        existed = set()
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                for ln in f:
                    s = ln.strip()
                    if s:
                        existed.add(s)
        with open(path, "a", encoding="utf-8") as f:
            for s in lines:
                if s not in existed:
                    f.write(s + "\n")

def check_pages_one_round(tenant: str):
    fb = _get_tenant_cfg(tenant)["FB"]
    tz_name = _get_tenant_cfg(tenant)["FEISHU"]["TZ"]

    pairs = load_label_id_pairs(fb["MAPPING_FILE"])
    total = len(pairs)
    if total == 0:
        return 0, [], 0, []

    session = build_session(tenant)

    # page_id -> (label, owner)
    id_to_meta = {pid: (label, owner) for (label, pid, owner) in pairs}
    results = []

    ex = ThreadPoolExecutor(max_workers=max(1, int(fb["CONCURRENCY"])))
    futs = {
        ex.submit(probe_page, tenant, session, page_id): page_id
        for (_, page_id, _) in pairs
    }

    st = _st(tenant)

    try:
        deadline = time.monotonic() + int(fb.get("ROUND_TIMEOUT", 180))
        pending = set(futs.keys())
        done_idx = 0

        while pending:
            if st["CANCEL_EVENT"].is_set():
                for fut in pending:
                    pid = futs[fut]
                    label, owner = id_to_meta.get(pid, ("", "未知"))
                    results.append({
                        "page_id": pid,
                        "label": label,
                        "owner_name": owner,
                        "status": "CANCELLED",
                        "normal": -1,
                        "checked_at": now_iso()
                    })
                    print(f"[{now_local_str(tz_name)}] ⚠️ (cancel) {label}-{pid} -> CANCELLED")
                pending.clear()
                break

            if time.monotonic() >= deadline:
                for fut in pending:
                    pid = futs[fut]
                    label, owner = id_to_meta.get(pid, ("", "未知"))
                    results.append({
                        "page_id": pid,
                        "label": label,
                        "owner_name": owner,
                        "status": "TIMEOUT",
                        "normal": -1,
                        "checked_at": now_iso()
                    })
                    print(f"[{now_local_str(tz_name)}] ⚠️ (timeout) {label}-{pid} -> TIMEOUT")
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

                # 统一加上 label + owner（以 pages.txt 为准）
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
                print(f"[{now_local_str(tz_name)}] {tag} ({done_idx}/{total}) {label}-{r['page_id']}{nm} -> {r.get('status')}{suffix}")

    finally:
        ex.shutdown(wait=False, cancel_futures=True)
        try:
            session.close()
        except:
            pass

    def _pack_item(row: Dict[str, Any]) -> Dict[str, str]:
        owner = (row.get("owner_name") or "").strip() or "未知"
        status = (row.get("status") or "").upper()
        page_id = (row.get("page_id") or "").strip()
        return {"owner": owner, "status": status, "page_id": page_id}

    # 三态写入文件
    tri_lines = []
    for r in results:
        stt = (r.get("status") or "").upper()
        if not r.get("label"):
            continue
        bucket = _bucket(stt)
        if bucket == "tri_state":
            tri_lines.append(f"{_fmt_label_id(r)} | {stt}")
        elif bucket == "ignored":
            tri_lines.append(f"{_fmt_label_id(r)} | OK")

    append_unique_lines(fb["OUT_TXT"], tri_lines)

    ok_count = sum(
        1 for r in results
        if ((r.get("status") or "").upper() == "OK") or (_bucket(r.get("status")) == "ignored")
    )

    certain_ab_items = [
        _pack_item(r) for r in results
        if r.get("label") and _bucket(r.get("status")) == "certain_abnormal"
    ]

    tech_items = [
        _pack_item(r) for r in results
        if r.get("label") and _bucket(r.get("status")) == "tech_exception"
    ]

    return ok_count, certain_ab_items, total, tech_items

# ==========================================================
# Feishu：tenant_access_token 缓存 + 发送
# ==========================================================
def _get_tenant_access_token(tenant: str) -> Optional[str]:
    cfg = _get_tenant_cfg(tenant)["FEISHU"]
    domain = cfg.get("DOMAIN", "feishu")
    base = _feishu_base(domain)

    st = _st(tenant)
    cache = st["TENANT_TOKEN_CACHE"]
    if cache["token"] and cache["expire_at"] > time.time() + 60:
        return cache["token"]

    url = base + "/open-apis/auth/v3/tenant_access_token/internal"
    resp = requests.post(
        url,
        json={"app_id": cfg["APP_ID"], "app_secret": cfg["APP_SECRET"]},
        timeout=8
    )
    try:
        data = resp.json()
    except Exception:
        data = {}

    if data.get("code") == 0 and data.get("tenant_access_token"):
        cache["token"] = data["tenant_access_token"]
        cache["expire_at"] = time.time() + int(data.get("expire", 7200))
        return cache["token"]

    print(f"[Feishu] tenant={tenant} 获取 tenant_access_token 失败：{data}")
    return None

def _send_text(tenant: str, chat_id: str, text: str):
    cfg = _get_tenant_cfg(tenant)["FEISHU"]
    domain = cfg.get("DOMAIN", "feishu")
    base = _feishu_base(domain)

    token = _get_tenant_access_token(tenant)
    if not token or not chat_id:
        return

    url = base + "/open-apis/im/v1/messages?receive_id_type=chat_id"
    maxlen = int(cfg.get("MAX_TEXT_LEN", 1800))
    chunks = [text[i:i+maxlen] for i in range(0, len(text), maxlen)] or [text]

    for chunk in chunks:
        body = {"receive_id": chat_id, "msg_type": "text",
                "content": json.dumps({"text": chunk}, ensure_ascii=False)}
        requests.post(url, headers={"Authorization": f"Bearer {token}"}, json=body, timeout=8)

def _target_chat_ids(tenant: str, preferred: Optional[str] = None):
    if preferred:
        return [preferred]

    # 支持 env 覆盖（多 chat id）
    ids_env = os.getenv(f"{tenant}_FEISHU_DEFAULT_CHAT_IDS", "") or os.getenv("FEISHU_DEFAULT_CHAT_IDS", "")
    ids = [x.strip() for x in re.split(r"[,\s;]+", ids_env) if x.strip()]

    cfg = _get_tenant_cfg(tenant)["FEISHU"]
    if not ids:
        one = os.getenv(f"{tenant}_FEISHU_DEFAULT_CHAT_ID") or cfg.get("DEFAULT_CHAT_ID", "")
        if one:
            ids = [one]

    st = _st(tenant)
    if not ids and st["CURRENT_CHAT_ID"]:
        ids = [st["CURRENT_CHAT_ID"]]

    uniq, seen = [], set()
    for x in ids:
        if x and x not in seen:
            uniq.append(x)
            seen.add(x)
    return uniq

# ==========================================================
# 推送：按 (Owner, Status) 分组（保留你原格式）
# ==========================================================
def _group_owner_status_to_ids(items: list[dict]) -> list[tuple[str, str, list[str]]]:
    from collections import defaultdict
    mp = defaultdict(list)
    for it in items:
        owner = (it.get("owner") or "未知").strip() or "未知"
        status = (it.get("status") or "UNKNOWN").strip().upper()
        pid = (it.get("page_id") or "").strip()
        if pid:
            mp[(owner, status)].append(pid)

    grouped = []
    for (owner, status), ids in mp.items():
        uniq_ids = sorted(set(ids))
        grouped.append((owner, status, uniq_ids))

    grouped.sort(key=lambda x: (-len(x[2]), x[0].lower(), x[1]))
    return grouped

def _append_group_block(lines: list[str], owner: str, status: str, ids: list[str], max_ids: int):
    owner_disp = owner or "未知"
    status_disp = status or "UNKNOWN"
    lines.append(f"{owner_disp} | {status_disp} | {len(ids)}")
    shown = ids[:max_ids]
    lines.extend(shown)
    if len(ids) > max_ids:
        lines.append(f"... 还有 {len(ids) - max_ids} 个 page_id 未展示（可调 FEISHU_MAX_IDS_PER_GROUP / FEISHU.MAX_IDS_PER_GROUP）")
    lines.append("")

def push_summary(tenant: str, round_name: str, ok: int, ab_items: list[dict],
                 chat_id: Optional[str] = None,
                 started_at: datetime | None = None,
                 ended_at: datetime | None = None,
                 duration_sec: int | None = None,
                 tech_items: list[dict] | None = None):
    tech_items = tech_items or []
    cfg = _get_tenant_cfg(tenant)["FEISHU"]

    targets = _target_chat_ids(tenant, chat_id)
    title = f"【FB Page 监控】{round_name}"

    lines = [title]
    if started_at and ended_at:
        lines += [
            f"开始：{started_at.strftime('%Y-%m-%d %H:%M:%S')}",
            f"结束：{ended_at.strftime('%Y-%m-%d %H:%M:%S')}（耗时{duration_sec or 0}s）",
        ]
        shown_time = ended_at.strftime("%Y-%m-%d %H:%M:%S")
    else:
        shown_time = now_local_str(cfg["TZ"])
        lines.append(f"时间：{shown_time}")

    lines.append(f"正常(OK)：{ok}")
    lines.append("")

    max_ids = int(cfg.get("MAX_IDS_PER_GROUP", 200))
    max_groups = int(cfg.get("MAX_GROUPS", 200))

    if ab_items:
        lines.append(f"确定异常：{len(ab_items)}")
        groups = _group_owner_status_to_ids(ab_items)
        for (owner, status, ids) in groups[:max_groups]:
            _append_group_block(lines, owner, status, ids, max_ids)
        if len(groups) > max_groups:
            lines.append(f"... 还有 {len(groups) - max_groups} 个分组未展示（可调 FEISHU_MAX_GROUPS / FEISHU.MAX_GROUPS）")
            lines.append("")

    if tech_items:
        lines.append(f"运行异常/权限类：{len(tech_items)}")
        groups2 = _group_owner_status_to_ids(tech_items)
        for (owner, status, ids) in groups2[:max_groups]:
            _append_group_block(lines, owner, status, ids, max_ids)
        if len(groups2) > max_groups:
            lines.append(f"... 还有 {len(groups2) - max_groups} 个分组未展示（可调 FEISHU_MAX_GROUPS / FEISHU.MAX_GROUPS）")
            lines.append("")

    if (not ab_items and not tech_items) and (not bool(cfg.get("PUSH_ON_NO_ABNORMAL", False))):
        return

    msg = "\n".join(lines).rstrip()
    for tgt in targets:
        _send_text(tenant, tgt, msg)

    st = _st(tenant)
    st["LAST_SUMMARY"].update({"time": shown_time, "ok": ok, "ab": len(ab_items), "source": round_name})
    print(f"[PUSH] tenant={tenant} {round_name}: ok={ok} ab={len(ab_items)} tech={len(tech_items)} to={targets}")

# ==========================================================
# 执行：互斥锁 + 周期 + 手动补跑（保留你原逻辑）
# ==========================================================
def run_once_with_lock(tenant: str, source: str, chat_id: Optional[str], notify_start: bool = False):
    st = _st(tenant)
    cfg = _get_tenant_cfg(tenant)
    tz = ZoneInfo(cfg["FEISHU"]["TZ"])

    if not st["RUN_MUTEX"].acquire(blocking=False):
        healed = _try_unstick_mutex(tenant, force=False)
        if healed and st["RUN_MUTEX"].acquire(blocking=False):
            print(f"[WATCHDOG] tenant={tenant} lock was stale; recreated and re-acquired successfully")
        else:
            if chat_id:
                _send_text(tenant, chat_id, "上一轮仍在执行，本次已跳过")
            print(f"[RUN] tenant={tenant} skip overlapped: {source} | active={st['RUN_ACTIVE'].is_set()} "
                  f"locked={st['RUN_MUTEX'].locked()} holder={_holder(tenant)} stuck_for={_lock_stuck_for(tenant):.1f}s")
            return False

    st["RUN_HOLDER"] = f"{source}::{threading.current_thread().name}"

    try:
        st["RUN_ACTIVE"].set()
        start_mono = time.monotonic()
        st["RUN_START_AT"] = start_mono
        st["RUN_SOURCE"] = source
        st["CANCEL_EVENT"].clear()

        start_wall = datetime.now(tz)
        print(f"[RUN] tenant={tenant} start {source}: at {start_wall.strftime('%Y-%m-%d %H:%M:%S')}")

        if notify_start and chat_id:
            _send_text(tenant, chat_id, "已开始执行，稍后回报结果")

        ok, ab_items, total, tech_items = check_pages_one_round(tenant)

        end_wall = datetime.now(tz)
        duration = max(0, int(time.monotonic() - start_mono))
        print(f"[RUN] tenant={tenant} end {source}: total={total} ok={ok} ab={len(ab_items)} tech={len(tech_items)} "
              f"start={start_wall.strftime('%Y-%m-%d %H:%M:%S')} end={end_wall.strftime('%Y-%m-%d %H:%M:%S')} cost={duration}s")

        push_summary(tenant, source, ok, ab_items, chat_id=chat_id,
                     started_at=start_wall, ended_at=end_wall, duration_sec=duration, tech_items=tech_items)
        return True

    except Exception as e:
        traceback.print_exc()
        if chat_id:
            _send_text(tenant, chat_id, f"{source}失败：{e}")
        return False

    finally:
        st["RUN_ACTIVE"].clear()
        st["RUN_SOURCE"] = ""
        st["RUN_HOLDER"] = ""
        try:
            st["RUN_MUTEX"].release()
        except Exception:
            pass

def _drain_manual(tenant: str):
    st = _st(tenant)
    while st["MANUAL_PENDING"].is_set():
        st["MANUAL_PENDING"].clear()
        with st["PENDING_LOCK"]:
            cid = st["PENDING_CHAT_ID"]
        print(f"[PENDING] tenant={tenant} draining manual -> run_once chat={cid}")
        run_once_with_lock(tenant, "手动执行", cid, notify_start=True)

def monitor_loop(tenant: str):
    st = _st(tenant)
    cfg = _get_tenant_cfg(tenant)
    interval = int(cfg["SCHEDULE"]["INTERVAL_SECONDS"])

    while st["RUN_FLAG"].is_set():
        run_once_with_lock(tenant, "周期执行", None)
        _drain_manual(tenant)

        for _ in range(interval):
            _try_unstick_mutex(tenant, force=False)
            if (not st["RUN_FLAG"].is_set()) or st["MANUAL_PENDING"].is_set() or st["CANCEL_EVENT"].is_set():
                print(f"[MON] tenant={tenant} break wait: pause={not st['RUN_FLAG'].is_set()} "
                      f"manual={st['MANUAL_PENDING'].is_set()} cancel={st['CANCEL_EVENT'].is_set()}")
                break
            time.sleep(1)

        if st["RUN_FLAG"].is_set() and st["MANUAL_PENDING"].is_set():
            _drain_manual(tenant)

def _start_monitor_if_needed(tenant: str):
    st = _st(tenant)
    if st["MONITOR_THREAD_STARTED"]:
        return
    if os.getenv("BOT_AUTOSTART", "false").lower() == "true":
        st["RUN_FLAG"].set()
        threading.Thread(target=monitor_loop, args=(tenant,), daemon=True, name=f"monitor-{tenant}").start()
        st["MONITOR_THREAD_STARTED"] = True
        print(f"[BOOT] tenant={tenant} 自动轮询已启动（间隔 { _get_tenant_cfg(tenant)['SCHEDULE']['INTERVAL_SECONDS'] }s）")

# 启动时尝试自启
for t in TENANTS.keys():
    _start_monitor_if_needed(t)

# ==========================================================
# Feishu 事件去重
# ==========================================================
def _seen_event(tenant: str, event_id: str = None, message_id: str = None) -> bool:
    st = _st(tenant)
    key = event_id or message_id
    if not key:
        return False
    now = time.time()
    with st["EVENT_LOCK"]:
        cutoff = now - EVENT_CACHE_TTL
        cache = st["EVENT_CACHE"]
        for k, ts in list(cache.items()):
            if ts < cutoff:
                cache.pop(k, None)
            else:
                break
        if key in cache:
            return True
        cache[key] = now
        if len(cache) > 2000:
            cache.popitem(last=False)
        return False

# ==========================================================
# Web 路由
# ==========================================================
@APP.route("/", methods=["GET"])
def home():
    return "ok", 200

@APP.route("/healthz", methods=["GET"])
def healthz():
    return "ok page-bot multi-tenant", 200

@APP.route("/test_send/<tenant>", methods=["GET"])
def test_send(tenant: str):
    cfg = _get_tenant_cfg(tenant)["FEISHU"]
    cid = request.args.get("cid") or cfg.get("DEFAULT_CHAT_ID") or _st(tenant)["CURRENT_CHAT_ID"]
    if not cid:
        return "no chat_id; provide ?cid=oc_xxx or set FEISHU.DEFAULT_CHAT_ID", 400
    try:
        _send_text(tenant, cid, f"测试：{tenant} Page 机器人发消息 OK")
        return "sent", 200
    except Exception as e:
        return (f"send failed: {e}"), 500

# ==========================================================
# 飞书事件入口（多租户）：/feishu/<tenant>/events
# ==========================================================
@APP.route("/feishu/<tenant>/events", methods=["POST"])
def feishu_events(tenant: str):
    if tenant not in TENANTS:
        return jsonify({"error": "unknown tenant"}), 404

    data = request.get_json(force=True, silent=True) or {}
    print(f"[EVENT RAW] tenant={tenant} keys={list(data.keys())}")

    cfg = _get_tenant_cfg(tenant)["FEISHU"]

    # token 校验（兼容 token 在 body 或 header.token）
    vt = cfg.get("VERIFICATION_TOKEN", "")
    got = data.get("token") or (data.get("header") or {}).get("token")
    if vt and got != vt:
        print(f"[EVENT] tenant={tenant} verification token mismatch")
        return jsonify({"code": 1, "msg": "invalid token"}), 403

    # url_verification
    if data.get("type") == "url_verification":
        return jsonify({"challenge": data.get("challenge")})

    header = data.get("header") or {}
    event_id = header.get("event_id")

    chat_id, message_id, text = None, None, ""
    content_raw, mentions, chat_type = "{}", [], "group"

    # schema 2.0
    if data.get("schema") == "2.0" and header.get("event_type") == "im.message.receive_v1":
        msg = (data.get("event", {}) or {}).get("message", {}) or {}
        message_id = msg.get("message_id")
        chat_id = msg.get("chat_id")
        chat_type = msg.get("chat_type") or "group"
        mentions = msg.get("mentions") or []
        content_raw = msg.get("content") or "{}"
        try:
            text = json.loads(content_raw).get("text", "")
        except Exception as e:
            print("[EVENT] parse content error:", e, content_raw)
            text = ""
    else:
        # legacy
        ev = data.get("event", {}) or {}
        msg = ev.get("message", {}) or {}
        event_id = event_id or ev.get("uuid") or msg.get("message_id")
        message_id = msg.get("message_id")
        chat_id = msg.get("chat_id")
        chat_type = msg.get("chat_type") or "group"
        mentions = msg.get("mentions") or []
        content_raw = msg.get("content") or "{}"
        try:
            text = json.loads(content_raw).get("text", "")
        except Exception as e:
            print("[EVENT] parse content error (legacy):", e, content_raw)
            text = ""

    # 去重
    if _seen_event(tenant, event_id=event_id, message_id=message_id):
        print(f"[DEDUP] tenant={tenant} drop event_id={event_id} message_id={message_id}")
        return jsonify({"code": 0})

    if not chat_id:
        print(f"[EVENT] tenant={tenant} no chat_id found, skip")
        return jsonify({"code": 0})

    # 群消息 require_at
    mentioned = bool(mentions)
    if not mentioned:
        try:
            mentioned = "@_user_" in (json.loads(content_raw).get("text", "") or "")
        except Exception:
            pass

    require_at = bool(cfg.get("REQUIRE_AT", True))
    if chat_type == "group" and require_at and not mentioned:
        print(f"[EVENT] tenant={tenant} ignore group message without @mention")
        return jsonify({"code": 0})

    text = clean_text(text)
    cmd = _norm_cmd(text)
    cmd_l = cmd.lower()
    print(f"[EVENT PARSED] tenant={tenant} chat_id={chat_id} text={repr(text)} cmd={repr(cmd)}")

    # 更新 current chat id
    st = _st(tenant)
    st["CURRENT_CHAT_ID"] = chat_id

    # 指令分发
    if cmd in ("chatid", "群id", "群ID"):
        _send_text(tenant, chat_id, f"chat_id: {chat_id}")

    elif cmd in ("执行", "立即执行") or cmd_l == "run":
        def _try_now():
            ok = run_once_with_lock(tenant, "手动执行", chat_id, notify_start=True)
            if not ok:
                with st["PENDING_LOCK"]:
                    st["PENDING_CHAT_ID"] = chat_id
                st["MANUAL_PENDING"].set()
                _send_text(tenant, chat_id, "当前正在执行，本轮结束后将立即补跑一次")
        threading.Thread(target=_try_now, daemon=True).start()

    elif cmd in ("开始", "start"):
        if not st["RUN_FLAG"].is_set():
            st["RUN_FLAG"].set()
            if not st["MONITOR_THREAD_STARTED"]:
                threading.Thread(target=monitor_loop, args=(tenant,), daemon=True, name=f"monitor-{tenant}").start()
                st["MONITOR_THREAD_STARTED"] = True
            _send_text(tenant, chat_id, f"监控已启动（间隔 {_get_tenant_cfg(tenant)['SCHEDULE']['INTERVAL_SECONDS']}s）")
        else:
            _send_text(tenant, chat_id, "监控已在运行中")

    elif cmd in ("暂停", "停止", "stop"):
        st["RUN_FLAG"].clear()
        _send_text(tenant, chat_id, "监控已暂停")

    elif cmd in ("中止", "取消本轮", "abort", "cancel"):
        if st["RUN_ACTIVE"].is_set():
            st["CANCEL_EVENT"].set()
            _send_text(tenant, chat_id, "已请求中止当前轮，等待释放…")
            for _ in range(3):
                if not st["RUN_MUTEX"].locked():
                    break
                time.sleep(1)
            if st["RUN_MUTEX"].locked():
                _try_unstick_mutex(tenant, force=True)
                _send_text(tenant, chat_id, "未及时释放，已强制解锁")
        else:
            if st["RUN_MUTEX"].locked():
                _try_unstick_mutex(tenant, force=True)
                _send_text(tenant, chat_id, "当前未在执行，但检测到锁异常，已强制解锁")
            else:
                _send_text(tenant, chat_id, "当前未在执行中，无需中止")

    elif cmd in ("抢占执行", "force", "force-run"):
        def _force():
            st["CANCEL_EVENT"].set()
            _send_text(tenant, chat_id, "已请求中止当前轮，准备抢占执行…")
            for _ in range(5):
                if not st["RUN_MUTEX"].locked():
                    break
                time.sleep(1)
            ok = run_once_with_lock(tenant, "手动执行", chat_id, notify_start=True)
            if not ok:
                with st["PENDING_LOCK"]:
                    st["PENDING_CHAT_ID"] = chat_id
                st["MANUAL_PENDING"].set()
                _send_text(tenant, chat_id, "当前仍在释放中，已加入队列，稍后自动补跑")
        threading.Thread(target=_force, daemon=True).start()

    elif cmd in ("重置锁", "reset-lock", "unlock"):
        _try_unstick_mutex(tenant, force=True)
        _send_text(tenant, chat_id, f"已强制重置互斥锁（holder={_holder(tenant)}）")

    elif cmd in ("状态", "status"):
        running = st["RUN_FLAG"].is_set()
        executing = st["RUN_ACTIVE"].is_set()
        locked = st["RUN_MUTEX"].locked()
        elapsed = _run_elapsed_sec(tenant) if executing else 0
        src = st["RUN_SOURCE"] or "无"
        holder = _holder(tenant)
        interval = _get_tenant_cfg(tenant)["SCHEDULE"]["INTERVAL_SECONDS"]
        last = st["LAST_SUMMARY"]
        lines = [
            f"Tenant：{tenant}",
            f"运行：{running}",
            f"执行中：{executing}（来源：{src}，已耗时 {elapsed}s）",
            f"锁占用：{locked}（持有者：{holder}）",
            f"间隔：{interval}s",
            f"上次：{last.get('time') or '无'}（OK={last.get('ok',0)}, AB={last.get('ab',0)}）",
        ]
        _send_text(tenant, chat_id, "\n".join(lines))

    else:
        m = re.match(r"^(?:间隔|interval)\s*=\s*(\d+)$", cmd_l)
        if m:
            try:
                sec = int(m.group(1))
                _get_tenant_cfg(tenant)["SCHEDULE"]["INTERVAL_SECONDS"] = max(10, sec)
                _send_text(tenant, chat_id, f"已更新间隔为 {_get_tenant_cfg(tenant)['SCHEDULE']['INTERVAL_SECONDS']} 秒")
            except Exception:
                _send_text(tenant, chat_id, "格式错误，示例：间隔=3600")
        else:
            # 帮助（你原来没有专门 help，这里补一个，不影响其它）
            if cmd in ("帮助", "help", "?"):
                _send_text(
                    tenant, chat_id,
                    "支持指令：chatid / 状态 / 执行 / 开始 / 暂停 / 中止 / 抢占执行 / 间隔=3600 / 重置锁"
                )
            else:
                # 不刷屏：不回复也行；如果你希望像之前那样回复未知指令，可取消注释
                _send_text(
                    tenant, chat_id,
                    "❓未知指令\n支持：chatid / 状态 / 执行 / 开始 / 暂停 / 中止 / 抢占执行 / 间隔=3600 / 重置锁"
                )

    return jsonify({"code": 0})

# ==========================================================
# 本地启动（Render 用 gunicorn，不走这里也没关系）
# ==========================================================
def ensure_result_file(path: str):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "a", encoding="utf-8"):
        pass

if __name__ == "__main__":
    # 确保每个 tenant 的结果文件存在
    for t in TENANTS.keys():
        ensure_result_file(_get_tenant_cfg(t)["FB"]["OUT_TXT"])
    port = int(os.getenv("PORT", "3000"))
    APP.run(host="0.0.0.0", port=port)

# pages_feishu_bot.py
import os
import json
import threading
from datetime import datetime
from flask import Flask, request, jsonify
import requests
import re

# ==========================================================
# 基础配置
# ==========================================================
APP = Flask(__name__)
APP_START_TS = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

FEISHU_BASE = "https://open.feishu.cn"
TOKEN_URL = f"{FEISHU_BASE}/open-apis/auth/v3/tenant_access_token/internal"
SEND_MSG_URL = f"{FEISHU_BASE}/open-apis/im/v1/messages?receive_id_type=chat_id"

DEFAULT_INTERVAL = int(os.getenv("DEFAULT_INTERVAL", "3600"))

# ==========================================================
# 多企业（Tenant）配置（来自环境变量 TENANTS_JSON）
#
# ✅ 强制要求大写键名：
# TENANTS_JSON 示例：
# {
#   "entA": {
#     "FEISHU": {
#       "APP_ID": "...",
#       "APP_SECRET": "...",
#       "VERIFICATION_TOKEN": "...",
#       "DEFAULT_CHAT_ID": "oc_xxx"
#     },
#     "FILES": {
#       "PAGES": "pages.txt",
#       "RESULT": "result_pages.txt"
#     }
#   }
# }
# ==========================================================
TENANTS_ENV = os.getenv("TENANTS_JSON")
if not TENANTS_ENV:
    raise RuntimeError("TENANTS_JSON env var is required")

try:
    TENANTS = json.loads(TENANTS_ENV)
except Exception as e:
    raise RuntimeError(f"TENANTS_JSON parse error: {e}")

def _required(d: dict, key: str, err: str):
    """d[key] 必须存在且非空，否则抛出可读错误"""
    if key not in d or d[key] in (None, ""):
        raise RuntimeError(err)
    return d[key]

def _validate_tenants():
    if not isinstance(TENANTS, dict) or not TENANTS:
        raise RuntimeError("TENANTS_JSON must be a non-empty JSON object (dict)")

    for tenant, cfg in TENANTS.items():
        if not isinstance(cfg, dict):
            raise RuntimeError(f"{tenant} config must be an object")

        if "FEISHU" not in cfg or not isinstance(cfg["FEISHU"], dict):
            raise RuntimeError(f"{tenant} missing FEISHU config")

        feishu = cfg["FEISHU"]
        _required(feishu, "APP_ID", f"{tenant}.FEISHU missing APP_ID")
        _required(feishu, "APP_SECRET", f"{tenant}.FEISHU missing APP_SECRET")
        _required(feishu, "VERIFICATION_TOKEN", f"{tenant}.FEISHU missing VERIFICATION_TOKEN")
        # DEFAULT_CHAT_ID 可选，但建议配
        if "DEFAULT_CHAT_ID" in feishu and feishu["DEFAULT_CHAT_ID"] in (None, ""):
            raise RuntimeError(f"{tenant}.FEISHU DEFAULT_CHAT_ID is empty")

        # FILES 可选：不配则 run_check 会报可读错误
        if "FILES" in cfg:
            if not isinstance(cfg["FILES"], dict):
                raise RuntimeError(f"{tenant}.FILES must be an object")
            # 允许只配其中一个，但 run_check 用到会再校验
            for k in ("PAGES", "RESULT"):
                if k in cfg["FILES"] and cfg["FILES"][k] in (None, ""):
                    raise RuntimeError(f"{tenant}.FILES {k} is empty")

_validate_tenants()

# ==========================================================
# 全局状态（按 tenant 隔离）
# ==========================================================
STATE = {
    t: {
        "running": False,
        "last_run": None,
        "interval": DEFAULT_INTERVAL,
        "stop": False
    } for t in TENANTS
}

# ==========================================================
# 工具函数
# ==========================================================
def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def normalize_feishu_callback(body: dict):
    # url_verification
    if body.get("type") == "url_verification":
        return "verify", None, None

    # 新版结构
    if "header" in body and "event" in body:
        return "event", (body.get("header") or {}).get("event_type"), body.get("event")

    # 旧版兜底
    if body.get("type") == "event_callback":
        evt = body.get("event") or {}
        return "event", evt.get("type"), evt

    return "unknown", None, None


AT_RE = re.compile(r"<at[^>]*>.*?</at>", re.IGNORECASE)

def clean_text(text: str) -> str:
    if not text:
        return ""

    # 1️⃣ 去掉 <at>...</at>
    text = AT_RE.sub("", text)

    # 2️⃣ 把可能残留的 @_user_x 去掉
    text = re.sub(r"@_user_\d+", "", text)

    # 3️⃣ 多空格归一
    text = re.sub(r"\s+", " ", text)

    return text.strip()



# ==========================================================
# 飞书 API
# ==========================================================
def feishu_get_token(tenant: str) -> str:
    cfg = TENANTS[tenant]["FEISHU"]
    app_id = cfg["APP_ID"]
    app_secret = cfg["APP_SECRET"]

    resp = requests.post(
        TOKEN_URL,
        json={"app_id": app_id, "app_secret": app_secret},
        timeout=15
    )
    resp.raise_for_status()
    data = resp.json()
    token = data.get("tenant_access_token")
    if not token:
        raise RuntimeError(f"tenant_access_token missing in response: {data}")
    return token


def feishu_send_text(tenant: str, chat_id: str, text: str):
    token = feishu_get_token(tenant)
    resp = requests.post(
        SEND_MSG_URL,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        },
        json={
            "receive_id": chat_id,
            "msg_type": "text",
            "content": json.dumps({"text": text}, ensure_ascii=False)
        },
        timeout=15
    )
    # 不 raise_for_status 也行，但建议保留以便你看到具体错误
    resp.raise_for_status()
    return resp.json()


# ==========================================================
# Facebook Page 检测逻辑（示意，保留你原逻辑结构）
# ==========================================================
def run_check(tenant: str, chat_id: str):
    st = STATE[tenant]
    if st["running"]:
        # 避免并发执行重复
        try:
            feishu_send_text(tenant, chat_id, f"⏳【{tenant}】正在执行中，请稍后再试")
        except Exception:
            pass
        return

    st["running"] = True
    st["last_run"] = now_str()

    try:
        feishu_send_text(tenant, chat_id, f"▶️【{tenant}】开始检测 Page 状态…")

        cfg = TENANTS[tenant]
        files = cfg.get("FILES") or {}
        pages_file = files.get("PAGES")
        result_file = files.get("RESULT")

        if not pages_file or not result_file:
            raise RuntimeError(f"{tenant}.FILES missing PAGES/RESULT (need both)")

        ok, abnormal = 0, 0
        results = []

        if os.path.exists(pages_file):
            with open(pages_file, "r", encoding="utf-8") as f:
                pages = [x.strip() for x in f if x.strip()]
        else:
            pages = []

        # 这里保留你原“示意逻辑”：全部记为 OK
        for p in pages:
            ok += 1
            results.append(f"OK | {p}")

        with open(result_file, "w", encoding="utf-8") as f:
            f.write("\n".join(results))

        feishu_send_text(
            tenant,
            chat_id,
            f"✅【{tenant}】检测完成\n"
            f"OK: {ok}\n"
            f"异常: {abnormal}\n"
            f"时间: {st['last_run']}"
        )

    except Exception as e:
        # 任何异常都反馈到飞书，方便你远程排查
        try:
            feishu_send_text(tenant, chat_id, f"❌【{tenant}】执行失败：{e}")
        except Exception:
            pass
        print(f"[ERR] tenant={tenant} run_check error: {e}", flush=True)

    finally:
        st["running"] = False


# ==========================================================
# Web 基础路由
# ==========================================================
@APP.route("/")
def index():
    return "ok"


@APP.route("/healthz")
def healthz():
    return "ok multi-tenant"


# ==========================================================
# 飞书事件入口（多租户）
# ==========================================================
@APP.route("/feishu/<tenant>/events", methods=["POST"])
def feishu_events(tenant):
    if tenant not in TENANTS:
        return jsonify({"error": "unknown tenant"}), 404

    body = request.get_json(silent=True) or {}
    print(f"[FEISHU IN] tenant={tenant} keys={list(body.keys())}", flush=True)

    # ✅ 先处理 url_verification
    if body.get("type") == "url_verification":
        return jsonify({"challenge": body.get("challenge")})

    # ✅ 校验 token（只要 body 里有 token 就校验）
    token = body.get("token")
    vt = TENANTS[tenant]["FEISHU"]["VERIFICATION_TOKEN"]
    if token and token != vt:
        return jsonify({"error": "invalid token"}), 403

    kind, event_type, event = normalize_feishu_callback(body)

    if kind == "event" and event_type == "im.message.receive_v1":
        msg = (event or {}).get("message", {}) or {}
        chat_id = msg.get("chat_id")
        content = msg.get("content", "{}") or "{}"

        try:
            text = clean_text(json.loads(content).get("text", ""))
        except Exception:
            text = clean_text(content)

        print(f"[CMD] tenant={tenant} chat_id={chat_id} text='{text}'", flush=True)

        # 指令分发
        if text == "chatid":
            feishu_send_text(tenant, chat_id, f"chat_id = {chat_id}")

        elif text in ("状态", "status"):
            st = STATE[tenant]
            feishu_send_text(
                tenant,
                chat_id,
                f"📊【{tenant}】状态\n"
                f"运行中: {st['running']}\n"
                f"上次执行: {st['last_run']}\n"
                f"间隔: {st['interval']}s"
            )

        elif text in ("执行", "run"):
            threading.Thread(target=run_check, args=(tenant, chat_id), daemon=True).start()

        elif text == "暂停":
            STATE[tenant]["stop"] = True
            feishu_send_text(tenant, chat_id, "⏸ 已暂停自动执行")

        elif text == "恢复":
            STATE[tenant]["stop"] = False
            feishu_send_text(tenant, chat_id, "▶️ 已恢复自动执行")

        else:
            feishu_send_text(tenant, chat_id, "❓未知指令\n支持：chatid / 状态 / 执行 / 暂停 / 恢复")

    return jsonify({"ok": True})


# ==========================================================
# 启动
# ==========================================================
if __name__ == "__main__":
    APP.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))

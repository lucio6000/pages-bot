# pages_feishu_bot.py
import os
import json
import time
import threading
from datetime import datetime
from flask import Flask, request, jsonify
import requests

# ==========================================================
# 基础配置
# ==========================================================
APP = Flask(__name__)
APP_START_TS = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ==========================================================
# 多企业（Tenant）配置
# 👉 每个企业一套：飞书 + pages 文件
# ==========================================================
import os
import json

TENANTS_ENV = os.getenv("TENANTS_JSON")

if not TENANTS_ENV:
    raise RuntimeError("TENANTS_JSON env var is required")

try:
    TENANTS = json.loads(TENANTS_ENV)
except Exception as e:
    raise RuntimeError(f"TENANTS_JSON parse error: {e}")

# 可选：基础校验（强烈建议）
for tenant, cfg in TENANTS.items():
    if "FEISHU" not in cfg:
        raise RuntimeError(f"{tenant} missing FEISHU config")
    for k in ("app_id", "app_secret", "verification_token", "default_chat_id"):
        if k not in cfg["FEISHU"]:
            raise RuntimeError(f"{tenant}.FEISHU missing {k}")


# ==========================================================
# 全局状态（按 tenant 隔离）
# ==========================================================
STATE = {
    t: {
        "running": False,
        "last_run": None,
        "interval": int(os.getenv("DEFAULT_INTERVAL", "3600")),
        "stop": False
    } for t in TENANTS
}

# ==========================================================
# 工具函数
# ==========================================================
def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def feishu_get_token(tenant):
    cfg = TENANTS[tenant]["FEISHU"]
    resp = requests.post(
        "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
        json={
            "app_id": cfg["APP_ID"],
            "app_secret": cfg["APP_SECRET"]
        },
        timeout=10
    )
    resp.raise_for_status()
    return resp.json()["tenant_access_token"]


def feishu_send_text(tenant, chat_id, text):
    token = feishu_get_token(tenant)
    requests.post(
        "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        },
        json={
            "receive_id": chat_id,
            "msg_type": "text",
            "content": json.dumps({"text": text}, ensure_ascii=False)
        },
        timeout=10
    )


def normalize_feishu_callback(body):
    # url_verification
    if body.get("type") == "url_verification":
        return "verify", None, None

    # 新版结构
    if "header" in body and "event" in body:
        return "event", body["header"].get("event_type"), body["event"]

    # 旧版兜底
    if body.get("type") == "event_callback":
        return "event", body.get("event", {}).get("type"), body.get("event")

    return "unknown", None, None


def clean_text(text: str):
    # 去掉 <at> 标签
    return text.replace("\n", " ").strip()


# ==========================================================
# Facebook Page 检测逻辑（示意，保留你原逻辑）
# ==========================================================
def run_check(tenant, chat_id):
    st = STATE[tenant]
    st["running"] = True
    st["last_run"] = now()

    feishu_send_text(tenant, chat_id, f"▶️【{tenant}】开始检测 Page 状态…")

    pages_file = TENANTS[tenant]["FILES"]["PAGES"]
    result_file = TENANTS[tenant]["FILES"]["RESULT"]

    ok, abnormal = 0, 0
    results = []

    if os.path.exists(pages_file):
        with open(pages_file, "r", encoding="utf-8") as f:
            pages = [x.strip() for x in f if x.strip()]
    else:
        pages = []

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

    # ✅ 最稳：先处理 url_verification（不依赖 normalize）
    if body.get("type") == "url_verification":
        return jsonify({"challenge": body.get("challenge")})

    # 校验 token（verify 之后再校验更安全）
    token = body.get("token")
    if token and token != TENANTS[tenant]["FEISHU"]["VERIFICATION_TOKEN"]:
        return jsonify({"error": "invalid token"}), 403

    kind, event_type, event = normalize_feishu_callback(body)


    if kind == "event" and event_type == "im.message.receive_v1":
        msg = event.get("message", {})
        chat_id = msg.get("chat_id")
        content = msg.get("content", "{}")
        text = clean_text(json.loads(content).get("text", ""))

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
            threading.Thread(target=run_check, args=(tenant, chat_id)).start()
        elif text == "暂停":
            STATE[tenant]["stop"] = True
            feishu_send_text(tenant, chat_id, "⏸ 已暂停自动执行")
        elif text == "恢复":
            STATE[tenant]["stop"] = False
            feishu_send_text(tenant, chat_id, "▶️ 已恢复自动执行")
        else:
            feishu_send_text(
                tenant,
                chat_id,
                "❓未知指令\n支持：chatid / 状态 / 执行 / 暂停 / 恢复"
            )

    return jsonify({"ok": True})


# ==========================================================
# 启动
# ==========================================================
if __name__ == "__main__":
    APP.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))

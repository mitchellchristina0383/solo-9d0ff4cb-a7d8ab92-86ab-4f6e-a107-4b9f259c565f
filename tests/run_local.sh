#!/usr/bin/env bash
# 本地（无 Docker）运行器：两个独立进程 + 两个独立 SQLite 文件，等价模拟 compose 两端。
# 用法：
#   tests/run_local.sh start-all        启动两端（接收端 :8001，发送端 :8000）
#   tests/run_local.sh stop-all         停止两端
#   tests/run_local.sh test             启动并运行完整自测（含停机/重启场景）
#   tests/run_local.sh clean            停止并删除本地数据
# 环境变量：PY（Python 解释器，默认 /tmp/rn-venv/bin/python，不存在则回退 python3）
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY="${PY:-/tmp/rn-venv/bin/python}"
[ -x "$PY" ] || PY=python3
DATA_DIR="${DATA_DIR:-/tmp/rn-data}"
LOG_DIR="${LOG_DIR:-/tmp/rn-logs}"
mkdir -p "$DATA_DIR" "$LOG_DIR"

start_receiver() {
  DB_PATH="$DATA_DIR/receiver.db" nohup "$PY" -m uvicorn app.main:app \
    --host 127.0.0.1 --port 8001 --app-dir "$ROOT/receiver" \
    >>"$LOG_DIR/receiver.log" 2>&1 &
}
start_sender() {
  # 默认关闭启动自动核对，避免后台线程与慢回执等用例产生时序竞争；
  # 需要验证「重启自动核对」的场景通过 restart-sender-autoreconcile 钩子单独开启。
  DB_PATH="$DATA_DIR/sender.db" RECEIVER_URL="http://127.0.0.1:8001" \
  STARTUP_RECONCILE="${STARTUP_RECONCILE:-0}" \
    nohup "$PY" -m uvicorn app.main:app --host 127.0.0.1 --port 8000 --app-dir "$ROOT/sender" \
    >>"$LOG_DIR/sender.log" 2>&1 &
}
start_sender_autoreconcile() {
  STARTUP_RECONCILE=1 start_sender
}
stop_receiver() { pkill -f "uvicorn app.main:app --host 127.0.0.1 --port 8001" 2>/dev/null || true; }
stop_sender()   { pkill -f "uvicorn app.main:app --host 127.0.0.1 --port 8000" 2>/dev/null || true; }

case "${1:-}" in
  start-all) start_receiver; start_sender ;;
  stop-all) stop_receiver; stop_sender ;;
  stop-receiver) stop_receiver ;;
  start-receiver) start_receiver ;;
  restart-sender) stop_sender; sleep 1; start_sender ;;
  restart-sender-autoreconcile) stop_sender; sleep 1; start_sender_autoreconcile ;;
  clean) stop_receiver; stop_sender; rm -rf "$DATA_DIR" ;;
  test)
    "$0" stop-all || true
    rm -rf "$DATA_DIR"; mkdir -p "$DATA_DIR"
    start_receiver; start_sender
    RN_LIFECYCLE=1 \
    RN_HOOK_STOP_RECEIVER="$ROOT/tests/run_local.sh stop-receiver" \
    RN_HOOK_START_RECEIVER="$ROOT/tests/run_local.sh start-receiver" \
    RN_HOOK_RESTART_SENDER="$ROOT/tests/run_local.sh restart-sender" \
    RN_HOOK_RESTART_SENDER_AUTO="$ROOT/tests/run_local.sh restart-sender-autoreconcile" \
    python3 "$ROOT/tests/selftest.py" --lifecycle
    ;;
  *) echo "用法: $0 {start-all|stop-all|stop-receiver|start-receiver|restart-sender|restart-sender-autoreconcile|clean|test}"; exit 1 ;;
esac

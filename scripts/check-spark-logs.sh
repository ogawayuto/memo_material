#!/bin/bash

# Spark Streaming Job Log Checker
# Sparkストリーミングジョブのログを確認するユーティリティスクリプト

set -e

# 色付き出力
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# ヘルプメッセージ
show_help() {
    cat << EOF
Usage: $0 [COMMAND] [OPTIONS]

Sparkストリーミングジョブのログを確認するツール

Commands:
    deserialize     デシリアライズログを表示
    errors          エラーログのみを表示
    summary         処理サマリーを表示
    recent          最近のログを表示（全て）
    app APP_ID      特定のアプリケーションIDのログを表示
    live            リアルタイムログを監視
    help            このヘルプを表示

Options:
    -n NUM          表示する行数（デフォルト: 20）
    -m MINUTES      過去N分のログを表示（デフォルト: 10）

Examples:
    $0 deserialize              # デシリアライズログを表示
    $0 errors -n 50             # エラーログを50行表示
    $0 summary                  # 処理サマリーを表示
    $0 app app-20260101081204   # 特定アプリのログを表示
    $0 live                     # リアルタイム監視

EOF
}

# デフォルト値
LINES=20
MINUTES=10

# オプション解析
while [[ $# -gt 0 ]]; do
    case $1 in
        -n)
            LINES="$2"
            shift 2
            ;;
        -m)
            MINUTES="$2"
            shift 2
            ;;
        help|--help|-h)
            show_help
            exit 0
            ;;
        *)
            COMMAND="$1"
            shift
            ;;
    esac
done

# コマンドが指定されていない場合
if [ -z "$COMMAND" ]; then
    echo -e "${YELLOW}コマンドを指定してください${NC}"
    show_help
    exit 1
fi

# 最新のstderrファイルを取得する関数
get_latest_stderr() {
    docker exec spark-worker bash -c "
        find /opt/spark/work -name 'stderr' -type f -mmin -${MINUTES} 2>/dev/null | \
        sort | tail -1
    " 2>/dev/null
}

# 最新のstdoutファイルを取得する関数
get_latest_stdout() {
    docker exec spark-worker bash -c "
        find /opt/spark/work -name 'stdout' -type f -mmin -${MINUTES} 2>/dev/null | \
        sort | tail -1
    " 2>/dev/null
}

# デシリアライズログを表示
show_deserialize_logs() {
    echo -e "${BLUE}=== デシリアライズログ（最新${LINES}行）===${NC}"

    stderr_file=$(get_latest_stderr)

    if [ -z "$stderr_file" ]; then
        echo -e "${RED}エラー: 過去${MINUTES}分以内のログが見つかりません${NC}"
        exit 1
    fi

    docker exec spark-worker bash -c "
        tail -1000 '$stderr_file' 2>/dev/null | \
        grep -E '\[DESERIALIZE' | \
        tail -${LINES}
    " || echo -e "${YELLOW}デシリアライズログが見つかりません${NC}"
}

# エラーログのみを表示
show_error_logs() {
    echo -e "${RED}=== エラーログ（最新${LINES}行）===${NC}"

    stderr_file=$(get_latest_stderr)

    if [ -z "$stderr_file" ]; then
        echo -e "${RED}エラー: 過去${MINUTES}分以内のログが見つかりません${NC}"
        exit 1
    fi

    docker exec spark-worker bash -c "
        tail -1000 '$stderr_file' 2>/dev/null | \
        grep -E '\[DESERIALIZE ERROR\]|ERROR' | \
        tail -${LINES}
    " || echo -e "${GREEN}エラーログはありません${NC}"
}

# 処理サマリーを表示
show_summary() {
    echo -e "${GREEN}=== 処理サマリー ===${NC}"

    stderr_file=$(get_latest_stderr)

    if [ -z "$stderr_file" ]; then
        echo -e "${RED}エラー: 過去${MINUTES}分以内のログが見つかりません${NC}"
        exit 1
    fi

    echo -e "\n${BLUE}バッチ処理統計:${NC}"
    docker exec spark-worker bash -c "
        tail -1000 '$stderr_file' 2>/dev/null | \
        grep -E '\[DESERIALIZE BATCH\]' | \
        tail -10
    " || echo -e "${YELLOW}バッチログが見つかりません${NC}"

    echo -e "\n${BLUE}成功/エラー集計:${NC}"
    docker exec spark-worker bash -c "
        tail -1000 '$stderr_file' 2>/dev/null | \
        grep -E '\[DESERIALIZE BATCH\]' | \
        tail -20 | \
        awk '{
            total += \$4;
            success += \$6;
            errors += \$9;
        }
        END {
            print \"総処理レコード数: \" total;
            print \"成功: \" success;
            print \"エラー: \" errors;
            if (total > 0) {
                printf \"成功率: %.2f%%\n\", (success/total)*100;
            }
        }'
    " 2>/dev/null || echo "統計データなし"
}

# 最近のログを表示
show_recent_logs() {
    echo -e "${BLUE}=== 最近のログ（最新${LINES}行）===${NC}"

    stderr_file=$(get_latest_stderr)

    if [ -z "$stderr_file" ]; then
        echo -e "${RED}エラー: 過去${MINUTES}分以内のログが見つかりません${NC}"
        exit 1
    fi

    echo -e "${YELLOW}ログファイル: $stderr_file${NC}\n"

    docker exec spark-worker tail -${LINES} "$stderr_file" 2>/dev/null
}

# 特定のアプリケーションIDのログを表示
show_app_logs() {
    local app_id="$1"

    if [ -z "$app_id" ]; then
        echo -e "${RED}エラー: アプリケーションIDを指定してください${NC}"
        echo "例: $0 app app-20260101081204-0002"
        exit 1
    fi

    echo -e "${BLUE}=== アプリケーション ${app_id} のログ ===${NC}"

    # stderrを表示
    echo -e "\n${YELLOW}--- stderr ---${NC}"
    docker exec spark-worker bash -c "
        find /opt/spark/work -path '*/${app_id}/*/stderr' -type f 2>/dev/null | \
        xargs tail -${LINES} 2>/dev/null
    " || echo -e "${RED}ログが見つかりません${NC}"

    # stdoutを表示
    echo -e "\n${YELLOW}--- stdout ---${NC}"
    docker exec spark-worker bash -c "
        find /opt/spark/work -path '*/${app_id}/*/stdout' -type f 2>/dev/null | \
        xargs tail -${LINES} 2>/dev/null
    " || echo -e "${YELLOW}stdoutログはありません${NC}"
}

# リアルタイムログ監視
show_live_logs() {
    echo -e "${GREEN}=== リアルタイムログ監視（Ctrl+Cで終了）===${NC}"
    echo -e "${YELLOW}デシリアライズログをリアルタイム表示します...${NC}\n"

    docker exec spark-worker bash -c '
        while true; do
            latest_stderr=$(find /opt/spark/work -name "stderr" -type f -mmin -1 2>/dev/null | sort | tail -1)

            if [ -n "$latest_stderr" ]; then
                echo "[ログファイル: $latest_stderr]"
                tail -f "$latest_stderr" 2>/dev/null | grep --line-buffered -E "\[DESERIALIZE|\[BATCH"
            else
                echo "ログファイルを待機中..."
                sleep 5
            fi
        done
    '
}

# アプリケーションID一覧を表示
list_applications() {
    echo -e "${BLUE}=== 実行中/最近のアプリケーション ===${NC}\n"

    docker exec spark-worker bash -c '
        ls -lt /opt/spark/work/ 2>/dev/null | \
        grep "^d" | \
        grep "app-" | \
        head -10 | \
        awk "{print \$9}"
    ' || echo -e "${RED}アプリケーションが見つかりません${NC}"
}

# コマンド実行
case "$COMMAND" in
    deserialize)
        show_deserialize_logs
        ;;
    errors)
        show_error_logs
        ;;
    summary)
        show_summary
        ;;
    recent)
        show_recent_logs
        ;;
    app)
        show_app_logs "$@"
        ;;
    live)
        show_live_logs
        ;;
    list)
        list_applications
        ;;
    *)
        echo -e "${RED}エラー: 不明なコマンド '$COMMAND'${NC}"
        show_help
        exit 1
        ;;
esac

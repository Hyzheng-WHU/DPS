#!/bin/bash
# 定义第一个脚本的路径
EXPERIMENT_SCRIPT="./run_single_experiment.sh"
CHECKOUT_PATH="/home/t1/ServerlessDB/OpenWhisk系统代码/incubator-openwhisk/checkout_branch.sh"

# 检查第一个脚本是否存在并可执行
if [ ! -x "$EXPERIMENT_SCRIPT" ]; then
    echo "错误: 脚本 $EXPERIMENT_SCRIPT 不存在或不可执行"
    echo "请确保第一个脚本已保存并有执行权限 (chmod +x $EXPERIMENT_SCRIPT)"
    exit 1
fi

# 为日志创建新目录
LOG_DIR="./自动化实验日志/experiment_logs_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$LOG_DIR"
echo "实验日志将保存在目录: $LOG_DIR"

# 将实验调用部分保存到单独的日志文件
EXPERIMENT_CALLS_LOG="$LOG_DIR/1执行脚本内容存档.log"
# 提取run_experiment调用部分并保存
sed -n '/^# 示例实验调用/,$p' "$0" > "$EXPERIMENT_CALLS_LOG"
echo "实验调用内容已保存到: $EXPERIMENT_CALLS_LOG"

# 定义一个函数来运行单个实验
run_experiment() {
    local warm_time="$1"
    local schedule_strategy="$2"
    local remove_strategy="$3"
    local window_size="$4"
    local total_time_window="${5:-30s}"
    local least_save_containers="${6:-1}"
    local workload="$7"
    local sliding_window_size="${8:-60}"
    local sliding_step_size="${9:-5}"
    local trigger_threshold="${10:-12}"
    local prefix="${11:-}"
    local custom_suffix="${12:-}"

    echo "========================================"
    echo "开始实验:"
    echo "- warm_time: $warm_time"
    echo "- schedule_strategy: $schedule_strategy"
    echo "- remove_strategy: $remove_strategy"
    echo "- window_size: $window_size"
    echo "- total_time_window: $total_time_window"
    echo "- least_save_containers: $least_save_containers"
    echo "- workload: $workload"
    echo "- sliding_window_size: $sliding_window_size"
    echo "- sliding_step_size: $sliding_step_size"
    echo "- trigger_threshold: $trigger_threshold"
    echo "- prefix: ${prefix:-<无>}"
    echo "- custom_suffix: ${custom_suffix:-<无>}"
    echo "========================================"

    # 构建日志文件名
    local log_filename="experiment_${workload##*/}_${warm_time}_${schedule_strategy}_${remove_strategy}_${window_size}_ttw${total_time_window%s}_lsc${least_save_containers}_sw${sliding_window_size}_ss${sliding_step_size}_tt${trigger_threshold}"
    if [ -n "$prefix" ]; then
        log_filename="${prefix}_${log_filename}"
    fi
    if [ -n "$custom_suffix" ]; then
        log_filename="${log_filename}${custom_suffix}"
    fi
    LOG_FILE="$LOG_DIR/${log_filename}.log"

    # 构建命令参数
    local cmd_args=(
        "--warm-time" "$warm_time"
        "--schedule-strategy" "$schedule_strategy"
        "--remove-strategy" "$remove_strategy"
        "--window-size" "$window_size"
        "--total-time-window" "$total_time_window"
        "--least-save-containers" "$least_save_containers"
        "--workload" "$workload"
        "--sliding-window-size" "$sliding_window_size"
        "--sliding-step-size" "$sliding_step_size"
        "--trigger-threshold" "$trigger_threshold"
    )

    # prefix 和 suffix 放在最后
    if [ -n "$custom_suffix" ]; then
        cmd_args+=("--custom-suffix" "$custom_suffix")
    fi
    if [ -n "$prefix" ]; then
        cmd_args+=("--prefix" "$prefix")
    fi

    # 执行实验并记录日志
    "$EXPERIMENT_SCRIPT" "${cmd_args[@]}" 2>&1 | tee "$LOG_FILE"

    echo "实验完成!"
    echo "日志已保存至: $LOG_FILE"
    echo ""
    echo "等待 10 秒钟..."
    sleep 10
}

# 示例实验调用

# run_experiment "10min" "改wait" "none" "3s" "9s" "3" "./workload文件/3.13-Trace(截30min).json" "60" "5" "12" "严格min热容器数限制 & 3回收条件" ""
# run_experiment "10min" "改wait" "best" "10s" "30s" "3" "./workload文件/3.13-Trace.json" "60" "5" "12" "严格min热容器数限制 & 3回收条件" ""

# run_experiment "10min" "改wait" "svoc" "3s" "9s" "3" "./workload文件/4.1-21Trace.json" "60" "5" "12" "严格min热容器数限制 & 3回收条件" ""

# run_experiment "10min" "改wait" "km" "3s" "9s" "3" "./workload文件/4.1-21Trace.json" "60" "5" "12" "严格min热容器数限制 & 3回收条件" "新km"
# run_experiment "10min" "改wait" "km" "3s" "9s" "3" "./workload文件/3.13-Trace.json" "60" "5" "12" "严格min热容器数限制 & 3回收条件" "新km"

# run_experiment "10min" "改wait" "km" "3s" "9s" "3" "./workload文件/3.13-Trace(截30min).json" "60" "5" "12" "严格min热容器数限制 & 3回收条件" "新km"
# run_experiment "10min" "改wait" "km" "3s" "9s" "3" "./workload文件/3.13-Trace(截30min).json" "60" "5" "12" "严格min热容器数限制 & 3回收条件" "新km"
# run_experiment "10min" "改wait" "km" "3s" "9s" "2" "./workload文件/3.13-Trace(量÷3).json" "60" "5" "12" "严格min热容器数限制 & 3回收条件" "改新km"
# run_experiment "10min" "改wait" "km" "3s" "9s" "2" "./workload文件/3.13-Trace.json" "60" "5" "12" "严格min热容器数限制 & 3回收条件" "改新km"

# run_experiment "10min" "改wait" "svoc" "3s" "9s" "3" "./workload文件/3.13-Trace(量÷3).json" "60" "5" "12" "严格min热容器数限制 & 3回收条件" "改新km"
# run_experiment "10min" "改wait" "km" "3s" "9s" "3" "./workload文件/3.13-Trace(量÷3).json" "60" "5" "12" "严格min热容器数限制 & 3回收条件" "改新km"

# 新random和svoc，去除最低热容器数限制

# run_experiment "10min" "改wait" "random" "3s" "9s" "0" "./workload文件/3.13-Trace.json" "60" "5" "12" "严min热 & 3条件" "新新random"
# run_experiment "10min" "改wait" "svoc" "3s" "9s" "0" "./workload文件/3.13-Trace.json" "60" "5" "12" "严min热 & 3条件" "新svoc"
# run_experiment "10min" "改wait" "random" "3s" "9s" "0" "./workload文件/4.1-21Trace.json" "60" "5" "12" "严min热 & 3条件" "新新random"
# run_experiment "10min" "改wait" "svoc" "3s" "9s" "0" "./workload文件/4.1-21Trace.json" "60" "5" "12" "严min热 & 3条件" "新svoc"
# run_experiment "10min" "改wait" "none" "3s" "9s" "2" "./workload文件/4.8-21Trace(截15minPeak).json" "60" "5" "12" "严min热 & 3条件" ""
# run_experiment "10min" "改wait" "km" "3s" "9s" "2" "./workload文件/4.8-21Trace(截15minPeak).json" "60" "5" "12" "严min热 & 3条件" "改新km"
# run_experiment "10min" "改wait" "svoc" "3s" "9s" "2" "./workload文件/4.8-21Trace(截15minPeak).json" "60" "5" "12" "严min热 & 3条件" "新svoc"

# run_experiment "10min" "改wait" "none" "3s" "9s" "2" "./workload文件/4.8-21Trace(截15minPeak).json" "60" "5" "12" "严min热 & 3条件" ""
# run_experiment "10min" "改wait" "svoc" "3s" "9s" "2" "./workload文件/4.8-21Trace(截15minPeak).json" "60" "5" "12" "严min热 & 3条件" "新svoc"

# run_experiment "10min" "改wait" "none" "3s" "9s" "2" "./workload文件/4.10-21Trace(截20minPeak).json" "60" "5" "12" "严min热 & 3条件" ""
# run_experiment "10min" "改wait" "km" "3s" "9s" "5" "./workload文件/4.8-21Trace(截20minPeak).json" "60" "5" "12" "严min热 & 3条件" "改新km"
# run_experiment "10min" "改wait" "km" "3s" "9s" "6" "./workload文件/4.10-21Trace(截20minPeak).json" "60" "5" "12" "严min热 & 3条件" "改新km"
# run_experiment "10min" "改wait" "km" "3s" "9s" "7" "./workload文件/4.10-21Trace(截20minPeak).json" "60" "5" "12" "严min热 & 3条件" "改新km"
# run_experiment "10min" "改wait" "km" "3s" "9s" "8" "./workload文件/4.10-21Trace(截20minPeak).json" "60" "5" "12" "严min热 & 3条件" "改新km"
# run_experiment "10min" "改wait" "km" "3s" "9s" "4" "./workload文件/4.8-21Trace(截15minPeak).json" "60" "5" "12" "严min热 & 3条件" "改新km"
# run_experiment "10min" "改wait" "km" "3s" "9s" "5" "./workload文件/4.8-21Trace(截15minPeak).json" "60" "5" "12" "严min热 & 3条件" "改新km"
# run_experiment "10min" "改wait" "km" "3s" "9s" "6" "./workload文件/4.8-21Trace(截15minPeak).json" "60" "5" "12" "严min热 & 3条件" "改新km"
# run_experiment "10min" "改wait" "km" "3s" "9s" "7" "./workload文件/4.8-21Trace(截15minPeak).json" "60" "5" "12" "严min热 & 3条件" "改新km"
# run_experiment "10min" "改wait" "km" "3s" "9s" "8" "./workload文件/4.8-21Trace(截15minPeak).json" "60" "5" "12" "严min热 & 3条件" "改新km"

# run_experiment "10min" "改wait" "km" "3s" "9s" "2" "./workload文件/4.8-21Trace(截20minPeak).json" "60" "5" "12" "严min热 & 3条件" "改新km"
# run_experiment "10min" "改wait" "svoc" "3s" "9s" "2" "./workload文件/4.8-21Trace(截20minPeak).json" "60" "5" "12" "严min热 & 3条件" "新svoc"

# run_experiment "10min" "改wait" "random" "3s" "9s" "2" "./workload文件/4.8-21Trace(截15minPeak).json" "60" "5" "12" "严min热 & 3条件" "新新random"
# run_experiment "10min" "改wait" "random" "3s" "9s" "2" "./workload文件/4.8-21Trace(截20minPeak).json" "60" "5" "12" "严min热 & 3条件" "新新random"

run_experiment "10min" "改wait" "km" "1s" "3s" "2" "./workload文件/3.13-Trace.json" "60" "5" "12" "严min热 & 3条件" "改新km"
run_experiment "10min" "改wait" "km" "2s" "6s" "2" "./workload文件/3.13-Trace.json" "60" "5" "12" "严min热 & 3条件" "改新km"
run_experiment "10min" "改wait" "km" "4s" "12s" "2" "./workload文件/3.13-Trace.json" "60" "5" "12" "严min热 & 3条件" "改新km"
run_experiment "10min" "改wait" "km" "5s" "15s" "2" "./workload文件/3.13-Trace.json" "60" "5" "12" "严min热 & 3条件" "改新km"
run_experiment "10min" "改wait" "km" "6s" "18s" "2" "./workload文件/3.13-Trace.json" "60" "5" "12" "严min热 & 3条件" "改新km"

# echo "所有实验已完成！"
echo "实验日志保存在目录: $LOG_DIR"

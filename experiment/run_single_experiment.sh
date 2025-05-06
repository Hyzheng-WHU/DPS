#!/bin/bash

# 定义文件路径
REQUEST_RECORD_MANAGER_PATH="/home/t1/ServerlessDB/OpenWhisk系统代码/incubator-openwhisk/common/scala/src/main/scala/org/info/RequestRecordManager.scala"
TIME_PREDICTOR_PATH="/home/t1/ServerlessDB/OpenWhisk系统代码/incubator-openwhisk/common/scala/src/main/scala/org/info/TimePredictor.scala"
ALL_VARS_PATH="/home/t1/ServerlessDB/OpenWhisk系统代码/incubator-openwhisk/ansible/group_vars/all"
SCH_SCRIPT_PATH="/home/t1/ServerlessDB/OpenWhisk系统代码/incubator-openwhisk/sch.sh"
PYTHON_SCRIPT_PATH="/home/t1/experiment/使用动态workload文件进行测试.py"
REFRESH_SCRIPT_PATH="/home/t1/sqlite/refreshInfoAndRemoveDb.sh"

# 默认参数
WARM_TIME="10min"
SCHEDULE_STRATEGY="no_wait"
REMOVE_STRATEGY="none"
WINDOW_SIZE="10s"
LEAST_SAVE_CONTAINERS=1  # 新增参数: 最少保存容器数
WORKLOAD="default"
TOTAL_TIME_WINDOW="30s"  # 总时间窗口参数
CUSTOM_SUFFIX=""  # 自定义后缀参数

# 新增参数: 时间窗口大小、滑动步长、触发条件
SLIDING_WINDOW_SIZE=60  # 默认时间窗口大小（秒）
SLIDING_STEP_SIZE=5     # 默认滑动步长（秒）
TRIGGER_THRESHOLD=12    # 默认触发条件（连续多少个滑动窗口下降或持平）
PREFIX=""  # 前缀参数

# 解析命令行参数
while [[ $# -gt 0 ]]; do
  case $1 in
    --warm-time)
      WARM_TIME="$2"
      shift 2
      ;;
    --schedule-strategy)
      SCHEDULE_STRATEGY="$2"
      shift 2
      ;;
    --remove-strategy)
      REMOVE_STRATEGY="$2"
      shift 2
      ;;
    --window-size)
      WINDOW_SIZE="$2"
      shift 2
      ;;
    --workload)
      WORKLOAD="$2"
      shift 2
      ;;
    --total-time-window)
      TOTAL_TIME_WINDOW="$2"
      shift 2
      ;;
    --least-save-containers)  # 新增参数处理
      LEAST_SAVE_CONTAINERS="$2"
      shift 2
      ;;
    --sliding-window-size)  # 新增参数处理
      SLIDING_WINDOW_SIZE="$2"
      shift 2
      ;;
    --sliding-step-size)  # 新增参数处理
      SLIDING_STEP_SIZE="$2"
      shift 2
      ;;
    --trigger-threshold)  # 新增参数处理
      TRIGGER_THRESHOLD="$2"
      shift 2
      ;;
    --custom-suffix)
      CUSTOM_SUFFIX="$2"
      shift 2
      ;;
    --prefix)  # 新增prefix参数处理
      PREFIX="$2"
      shift 2
      ;;
    *)
      echo "未知参数: $1"
      exit 1
      ;;
  esac
done

# 提取时间数值用于规则判断
TIME_VALUE=$(echo "$WARM_TIME" | sed 's/[^0-9]//g')

# 提取window-size的数值部分
WINDOW_SIZE_VALUE=$(echo "$WINDOW_SIZE" | sed 's/[^0-9]//g')

# 提取total-time-window的数值部分
TOTAL_TIME_WINDOW_VALUE=$(echo "$TOTAL_TIME_WINDOW" | sed 's/[^0-9]//g')

# 参数验证和派生参数设置
if [[ "$TIME_VALUE" -ge 20 && ("$SCHEDULE_STRATEGY" != "no_wait" || "$REMOVE_STRATEGY" != "none") ]]; then
  echo "警告: warm_time为${TIME_VALUE}min (大于等于20min)时，只能选择no_wait和none参数"
  SCHEDULE_STRATEGY="no_wait"
  REMOVE_STRATEGY="none"
fi

if [[ "$SCHEDULE_STRATEGY" == "no_wait" && "$REMOVE_STRATEGY" != "none" ]]; then
  echo "警告: schedule_strategy为no_wait时，只能选择none参数"
  REMOVE_STRATEGY="none"
fi

# 设置Python脚本的containerStrategy和suffix参数
if [[ "$REMOVE_STRATEGY" == "none" ]]; then
  CONTAINER_STRATEGY="(被动)"
  # 如果提供了自定义后缀，则使用它，否则为空
  if [[ -n "$CUSTOM_SUFFIX" ]]; then
    SUFFIX="$CUSTOM_SUFFIX"
  else
    SUFFIX=""
  fi
else
  CONTAINER_STRATEGY="(${WINDOW_SIZE}[7_2_1]warm&working)"
  # 统一处理所有非none的情况
  if [[ -n "$CUSTOM_SUFFIX" ]]; then
    SUFFIX="_${REMOVE_STRATEGY}${CUSTOM_SUFFIX}"
  else
    SUFFIX="_${REMOVE_STRATEGY}"
  fi
fi

echo "执行参数设置:"
echo "- warm_time: $WARM_TIME"
echo "- schedule_strategy: $SCHEDULE_STRATEGY"
echo "- remove_strategy: $REMOVE_STRATEGY"
echo "- window_size: $WINDOW_SIZE"
echo "- least_save_containers: $LEAST_SAVE_CONTAINERS"  # 新增显示least_save_containers参数
echo "- workload: $WORKLOAD"
echo "- total_time_window: $TOTAL_TIME_WINDOW"
echo "- sliding_window_size: $SLIDING_WINDOW_SIZE"  # 新增显示时间窗口大小
echo "- sliding_step_size: $SLIDING_STEP_SIZE"    # 新增显示滑动步长
echo "- trigger_threshold: $TRIGGER_THRESHOLD"    # 新增显示触发条件
echo "- container_strategy: $CONTAINER_STRATEGY"
echo "- suffix: $SUFFIX"
echo "- custom_suffix: $CUSTOM_SUFFIX"
echo "- prefix: ${PREFIX:-<无>}"  # 显示prefix参数
echo ""

# 步骤1: 修改配置文件
echo "步骤1: 修改配置文件..."

# 修改idle container超时时间
TIME_VALUE=$(echo "$WARM_TIME" | sed 's/[^0-9]//g')
sed -i "s/idleContainer: \"{{ containerProxy_timeouts_idleContainer | default('.*') }}\"/idleContainer: \"{{ containerProxy_timeouts_idleContainer | default('$TIME_VALUE minutes') }}\"/g" "$ALL_VARS_PATH"
echo "已将idle container超时时间设置为$TIME_VALUE分钟"

# 修改ConsideredStates
if [[ "$SCHEDULE_STRATEGY" == "改wait" ]]; then
  sed -i 's/val ConsideredStates: List\[String\] = List(.*)/val ConsideredStates: List[String] = List("warm", "working", "loading", "prewarm")/g' "$TIME_PREDICTOR_PATH"
  echo "已将ConsideredStates设置为List(\"warm\", \"working\", \"loading\", \"prewarm\")"
else
  sed -i 's/val ConsideredStates: List\[String\] = List(.*)/val ConsideredStates: List[String] = List("warm", "prewarm")/g' "$TIME_PREDICTOR_PATH"
  echo "已将ConsideredStates设置为List(\"warm\", \"prewarm\")"
fi

# 修改removeStrategy
sed -i "s/val removeStrategy = \".*\"/val removeStrategy = \"$REMOVE_STRATEGY\"/g" "$REQUEST_RECORD_MANAGER_PATH"
echo "已将removeStrategy设置为\"$REMOVE_STRATEGY\""

# 修改containerCheckIntervalInSec
sed -i "s/val containerCheckIntervalInSec = [0-9]*/val containerCheckIntervalInSec = $WINDOW_SIZE_VALUE/g" "$REQUEST_RECORD_MANAGER_PATH"
echo "已将containerCheckIntervalInSec设置为$WINDOW_SIZE_VALUE秒"

# 修改totalTimeWindowInSec
sed -i "s/val totalTimeWindowInSec = [0-9]*/val totalTimeWindowInSec = $TOTAL_TIME_WINDOW_VALUE/g" "$REQUEST_RECORD_MANAGER_PATH"
echo "已将totalTimeWindowInSec设置为$TOTAL_TIME_WINDOW_VALUE秒"

# 修改leastSaveContainers
sed -i "s/val leastSaveContainers = [0-9]*/val leastSaveContainers = $LEAST_SAVE_CONTAINERS/g" "$REQUEST_RECORD_MANAGER_PATH"
echo "已将leastSaveContainers设置为$LEAST_SAVE_CONTAINERS"

# 新增修改slidingWindowSizeInSec
sed -i "s/val slidingWindowSizeInSec = [0-9]*/val slidingWindowSizeInSec = $SLIDING_WINDOW_SIZE/g" "$REQUEST_RECORD_MANAGER_PATH"
echo "已将slidingWindowSizeInSec设置为$SLIDING_WINDOW_SIZE秒"

# 新增修改slidingStepSizeInSec
sed -i "s/val slidingStepSizeInSec = [0-9]*/val slidingStepSizeInSec = $SLIDING_STEP_SIZE/g" "$REQUEST_RECORD_MANAGER_PATH"
echo "已将slidingStepSizeInSec设置为$SLIDING_STEP_SIZE秒"

# 新增修改triggerThreshold
sed -i "s/val triggerThreshold = [0-9]*/val triggerThreshold = $TRIGGER_THRESHOLD/g" "$REQUEST_RECORD_MANAGER_PATH"
echo "已将triggerThreshold设置为$TRIGGER_THRESHOLD"

echo "配置文件修改完成"

# 步骤2: 执行sch.sh脚本进行重新部署
echo "步骤2: 执行sch.sh脚本进行重新部署..."
bash "$SCH_SCRIPT_PATH"
echo "重新部署完成"

sleep 10

# 步骤3: 执行Python脚本进行实验并记录
echo "步骤3: 执行Python脚本进行实验..."
# 构建带有prefix、窗口值和suffix的命令
python3 "$PYTHON_SCRIPT_PATH" --schedule_strategy="$SCHEDULE_STRATEGY" --containerStrategy="$CONTAINER_STRATEGY" --warm_time="$WARM_TIME" --least_save_containers="$LEAST_SAVE_CONTAINERS" --workload="$WORKLOAD" --window_size="$WINDOW_SIZE_VALUE" --total_time_window="$TOTAL_TIME_WINDOW_VALUE" --sliding_window_size="$SLIDING_WINDOW_SIZE" --sliding_step_size="$SLIDING_STEP_SIZE" --trigger_threshold="$TRIGGER_THRESHOLD" --suffix="$SUFFIX" --prefix="$PREFIX"
echo "实验完成"

# 步骤4: 执行刷新脚本
echo "步骤4: 执行刷新脚本..."
echo "" | bash "$REFRESH_SCRIPT_PATH"
echo "刷新完成"

echo "所有步骤已完成！"

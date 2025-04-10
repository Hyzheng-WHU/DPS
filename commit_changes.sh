#!/bin/bash

# 检查是否传入了提交信息
if [ -z "$1" ]; then
    echo "错误：请输入提交信息作为参数。"
    echo "用法：./commit_changes.sh \"提交信息\""
    exit 1
fi

# 定义提交信息
commit_message="$1"

# 进入目标目录
cd /home/t1/incubator-openwhisk || { echo "无法进入 /home/t1/incubator-openwhisk 目录"; exit 1; }

# 获取当前分支名
current_branch=$(git rev-parse --abbrev-ref HEAD)

# 提示当前所在分支并等待用户确认
echo -e "当前所在分支为：$current_branch\n按 Enter 键确认上传本次更改，或 Ctrl+C 取消操作..."
read -r 

# 添加所有更改
git add /home/t1/incubator-openwhisk

# 提交更改
git commit -m "$commit_message"

# 推送更改到远程仓库的当前分支
git push origin "$current_branch"

# 提示操作完成
echo "提交并推送到分支 $current_branch 完成！"
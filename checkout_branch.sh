#!/bin/bash

# 检查是否提供了分支名作为参数
if [ -z "$1" ]; then
    echo "错误：请提供分支名作为参数。"
    echo "用法：./switch_branch_and_set_permissions.sh <branch_name>"
    exit 1
fi

# 获取参数
branch_name="$1"

# 切换到指定的分支
echo "切换到分支 $branch_name..."
git checkout "$branch_name" || { echo "切换分支失败，请检查分支名是否正确。"; exit 1; }

# 修改当前目录的权限
echo "设置当前目录及子目录权限为 777..."
chmod -R 777 . || { echo "修改权限失败，请检查是否有权限运行此脚本。"; exit 1; }

echo "分支切换和权限设置完成！"

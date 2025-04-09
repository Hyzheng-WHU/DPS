#!/bin/bash

# 脚本开始
echo "切换到 main 分支..."
git checkout main || { echo "切换到 main 分支失败"; exit 1; }

echo "从 no_wait 分支同步所有内容到 main（排除 ContainerManager.scala）..."
git checkout no_wait -- . || { echo "从 no_wait 分支同步失败"; exit 1; }

echo "恢复 main 分支的 ContainerManager.scala 文件..."
git checkout main -- /home/t1/ServerlessDB/OpenWhisk系统代码/incubator-openwhisk/core/scheduler/src/main/scala/org/apache/openwhisk/core/scheduler/container/ContainerManager.scala || { echo "恢复 ContainerManager.scala 失败"; exit 1; }

echo "恢复 main 分支的 ContainerDbInfoManager.scala 文件..."
git checkout main -- /home/t1/ServerlessDB/OpenWhisk系统代码/incubator-openwhisk/common/scala/src/main/scala/org/info/ContainerDbInfoManager.scala || { echo "恢复 ContainerDbInfoManager.scala 失败"; exit 1; }

echo "同步完成！"

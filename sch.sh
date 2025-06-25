#!/bin/bash

# Stop the script if any command fails
set -e

# First, run the database cleanup script with auto-confirmation
echo "正在清理数据库文件..."
echo "" | /home/t1/sqlite/refreshInfoAndRemoveDb.sh

echo "数据库清理完成，开始构建 OpenWhisk..."

# Change directory to the OpenWhisk project
cd /home/t1/incubator-openwhisk

# Build the scheduler
./gradlew :core:scheduler:build

# Create Docker distribution for the scheduler
./gradlew :core:scheduler:distDocker

# Change directory to the Ansible directory
cd /home/t1/incubator-openwhisk/ansible

# Run the Ansible playbook
ansible-playbook -i environments/local openwhisk.yml
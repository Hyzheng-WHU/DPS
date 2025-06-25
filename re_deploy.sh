echo "正在清理数据库文件..."
echo "" | /home/t1/sqlite/refreshInfoAndRemoveDb.sh

echo "数据库清理完成，开始构建 OpenWhisk..."

# Change directory to the Ansible directory
cd /home/t1/incubator-openwhisk/ansible

# Run the Ansible playbook
ansible-playbook -i environments/local openwhisk.yml
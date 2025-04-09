#!/bin/bash

# Stop the script if any command fails
set -e

# Change directory to the OpenWhisk project
cd /home/t1/incubator-openwhisk

# Build the scheduler
./gradlew :core:controller:build

# Create Docker distribution for the scheduler
./gradlew :core:controller:distDocker

# Change directory to the Ansible directory
cd /home/t1/incubator-openwhisk/ansible

# Run the Ansible playbook
ansible-playbook -i environments/local openwhisk.yml


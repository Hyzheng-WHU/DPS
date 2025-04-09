#!/bin/bash

# Stop the script if any command fails
set -e

# Change directory to the OpenWhisk project
cd /home/t1/incubator-openwhisk

# Build the controller
./gradlew :core:controller:build

# Create Docker distribution for the controller
./gradlew :core:controller:distDocker

# Build the scheduler
./gradlew :core:scheduler:build

# Create Docker distribution for the scheduler
./gradlew :core:scheduler:distDocker

# Build the invoker
./gradlew :core:invoker:build

# Create Docker distribution for the invoker
./gradlew :core:invoker:distDocker

# Change directory to the Ansible directory
cd /home/t1/incubator-openwhisk/ansible

# Run the Ansible playbook
ansible-playbook -i environments/local openwhisk.yml


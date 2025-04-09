#!/bin/bash

# Stop the script if any command fails
set -e

# Change directory to the OpenWhisk project
cd /home/t1/incubator-openwhisk

# Build the scheduler
./gradlew :core:scheduler:build

# Build the invoker
./gradlew :core:invoker:build
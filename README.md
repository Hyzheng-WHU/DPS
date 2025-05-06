# Data-centric and Proactive Scheduling for Serverless Databases

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)

This project presents a modified version of OpenWhisk that implements data-centric and proactive scheduling mechanisms for serverless databases. It focuses on optimizing performance by redesigning the scheduler and invoker components to better handle database workloads in serverless environments.

## Project Highlights

- **Enhanced Scheduler**: Reimplemented scheduling logic to be data-aware and proactive for database operations
- **Optimized Invoker**: Modified invoker component to better handle database-specific execution patterns
- **Workload Testing**: Includes database-specific workloads for performance evaluation
- **Experimental Scripts**: Ready-to-run scripts for reproducing experimental results

## System Requirements

__**Important**: Due to version dependency issues, Ubuntu 18.04 is strongly recommended for deployment.__

## Deployment Guide

This project follows the same deployment approach as OpenWhisk. For detailed deployment instructions, please refer to the [official OpenWhisk deployment documentation](https://github.com/apache/openwhisk/blob/master/ansible/README.md).

## Project Structure

The key modifications and additions to the original OpenWhisk codebase are as follows:

- **New Components**: Located in `/common/scala/src/main/scala/org/info/`
- **Modified Scheduler**: Most changes are in `/core/scheduler/src/main/scala/org/apache/openwhisk/core/scheduler/queue/`
- **Modified Invoker**: Most changes are in `/core/invoker/src/main/scala/org/apache/openwhisk/core/`
- **Workloads**: Test workloads can be found in `/workloads/`
- **Experiment Scripts**: Scripts for running experiments are in `/experiment/`
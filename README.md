# data-processor

## Overview

🌐🔒 The service provides a secure server handling user-specific data with quotas.

## Key Components

- ⚙️ **Workers**: Manage data processing in the background.
- 🛠️ **Data Processing**: Ingest and process incoming data.
- 🧑‍💻 **User Quotas**: Track and enforce data and request quotas per user.
- 🔄 **Periodic Tasks**: Save user quotas and clean up data periodically.
- ⏱️ **Timers**: Control operations through intervals and timeouts.
- 🛑 **Signal Handling**: Gracefully handle shutdown and cleanup.
- 📂 **File Operations**: Read and write user quota states to files.
- 🔑 **Hashing**: Generate unique identifiers for data entries.
- 🚦 **Request Handling**: Queue incoming data requests and enforce limits.

## Workflow

1. 🚦 Data is received through the `/input` endpoint.
2. 🧑‍💻 User quotas are checked and enforced.
3. ⚙️ Data is processed and passed to the worker pool.
4. 🛠️ Each worker processes and stores the data securely.
5. 🔄 Quota states and data maps are periodically saved and pruned.
6. 🛑 On shutdown signals, the server cleanly exits after ensuring all tasks are complete.

## Installation & Running

```shell
go build
./data-processor




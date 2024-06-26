#!/bin/bash

host_root_path="/home/mkhalilov/Development/flexio-sdk/"
host_binary_path="${host_root_path}/cc-host/apps/flexio_ag_bench/host/flexio_ag_bench"

daemon_cmd="${host_binary_path} --hmem_daemon --rdma_dev=mlx5_2"

until $daemon_cmd; do
    echo "Daemon crashed with exit code $?.  Respawning.." >&2
done

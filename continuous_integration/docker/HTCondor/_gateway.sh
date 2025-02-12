#!/usr/bin/env bash
set -xe

cd /working
dask-gateway-server --config=continuous_integration/docker/HTCondor/dask_gateway_config.py

#!/usr/bin/env bash
set -euo pipefail
docker build --platform linux/amd64 -t benzhi-dyl-04 -f benzhi.Dockerfile .

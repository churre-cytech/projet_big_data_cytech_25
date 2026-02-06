#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "[build_jars] Build ex01_data_retrieval..."
cd "${REPO_ROOT}/ex01_data_retrieval"
sbt -batch clean assembly

echo "[build_jars] Build ex02_data_ingestion..."
cd "${REPO_ROOT}/ex02_data_ingestion"
sbt -batch clean assembly

echo "[build_jars] OK - jars generated:"
echo "  - ex01_data_retrieval/target/scala-2.13/ex01-retrieval-assembly.jar"
echo "  - ex02_data_ingestion/target/scala-2.13/ex02-ingestion-assembly.jar"

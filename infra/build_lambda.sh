#!/usr/bin/env bash
# Build the Lambda package: lakefreight plus the Snowflake connector, as Linux wheels.
#
#   infra/build_lambda.sh [x86_64|arm64]
#
# Writes build/lakefreight-lambda.zip. boto3 is not bundled; the Lambda runtime has it.
set -euo pipefail

arch="${1:-x86_64}"
case "$arch" in
  x86_64) platform="manylinux2014_x86_64" ;;
  arm64) platform="manylinux2014_aarch64" ;;
  *) echo "usage: $0 [x86_64|arm64]" >&2; exit 2 ;;
esac
python_version="3.13" # must match the runtime in infra/terraform/pipeline.tf

root="$(cd "$(dirname "$0")/.." && pwd)"
staging="$root/build/lambda"
zip_path="$root/build/lakefreight-lambda.zip"
rm -rf "$staging" "$zip_path"
mkdir -p "$staging"

python3 -m pip install --quiet --no-compile \
  --platform "$platform" --implementation cp --python-version "$python_version" \
  --only-binary=:all: --target "$staging" \
  "$root[snowflake]"

# Drop what Lambda never needs.
find "$staging" -type d -name "__pycache__" -prune -exec rm -rf {} +
rm -rf "$staging"/bin

(cd "$staging" && zip -q -r -X "$zip_path" .)
echo "wrote $zip_path ($(du -h "$zip_path" | cut -f1) zipped, $(du -sh "$staging" | cut -f1) unzipped)"

#!/bin/bash
set -e

# Configuration
AUTOSCALER_VERSION="master"
ROOT_PROTO_DIR="proto"
PACKAGE_PATH="verda_cloud_provider/gen/externalgrpc"
FULL_PROTO_DIR="$ROOT_PROTO_DIR/$PACKAGE_PATH"
SRC_ROOT="src"

# Clean setup
echo "🧹 Cleaning up..."
rm -rf "$ROOT_PROTO_DIR"
mkdir -p "$SRC_ROOT/$PACKAGE_PATH"
touch "$SRC_ROOT/$PACKAGE_PATH/__init__.py"

# Download
echo "⬇️  Downloading externalgrpc.proto..."
mkdir -p "$FULL_PROTO_DIR"
wget -q -O "$FULL_PROTO_DIR/externalgrpc.proto" \
  https://raw.githubusercontent.com/kubernetes/autoscaler/$AUTOSCALER_VERSION/cluster-autoscaler/cloudprovider/externalgrpc/protos/externalgrpc.proto

# Generate
echo "⚙️  Generating Python code..."
python -m grpc_tools.protoc \
  -I "$ROOT_PROTO_DIR" \
  --python_out="$SRC_ROOT" \
  --grpc_python_out="$SRC_ROOT" \
  --pyi_out="$SRC_ROOT" \
  "$FULL_PROTO_DIR/externalgrpc.proto"

echo "✅ Done! Code generated in $SRC_ROOT/$PACKAGE_PATH"

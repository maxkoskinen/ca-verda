#!/bin/bash
set -e

# --- Configuration ---

# --- Version Configuration ---
K8S_VERSION="1.30.0"
# Autoscaler uses same minor version as k8s
CA_VERSION="1.30.0"

# Derived tags (do not edit)
K8S_TAG="kubernetes-${K8S_VERSION}"
CA_TAG="cluster-autoscaler-${CA_VERSION}"
# couldnt get import paths to work for src/generated
# but with this we get src/clusterautoscaler & src/k8s andits enough for now
DEST_DIR="src"
TEMP_BUILD_DIR="proto_build_tmp"

# Submodule paths
SUB_API="third_party/kubernetes/api"
SUB_MACHINERY="third_party/kubernetes/apimachinery"
SUB_AUTOSCALER="third_party/kubernetes/autoscaler"

# Autoscaler Proto Source
AUTOSCALER_PROTO_SRC="$SUB_AUTOSCALER/cluster-autoscaler/cloudprovider/externalgrpc/protos/externalgrpc.proto"
AUTOSCALER_PKG_PATH="clusterautoscaler/cloudprovider/v1/externalgrpc"

# 1. Checkout correct submodule versions
echo "📌 Pinning submodules..."
git -C "$SUB_API"       fetch --tags --quiet
git -C "$SUB_MACHINERY" fetch --tags --quiet
git -C "$SUB_AUTOSCALER" fetch --tags --quiet

git -C "$SUB_API"        checkout "$K8S_TAG"
git -C "$SUB_MACHINERY"  checkout "$K8S_TAG"
git -C "$SUB_AUTOSCALER" checkout "$CA_TAG"

# 2. Verify go.mod agrees
GOMOD_API_VERSION=$(grep "k8s.io/api\b" "$SUB_AUTOSCALER/cluster-autoscaler/go.mod" | awk '{print $2}' | head -1)
EXPECTED_GOMOD_VERSION="v0.${K8S_VERSION#1.}"   # 1.30.0 → v0.30.0
EXPECTED_GOMOD_VERSION="${EXPECTED_GOMOD_VERSION%.*}.${K8S_VERSION##*.}"  # ensure patch

if [ "$GOMOD_API_VERSION" != "$EXPECTED_GOMOD_VERSION" ]; then
  echo "⚠️  Warning: autoscaler go.mod has k8s.io/api=${GOMOD_API_VERSION}, expected ${EXPECTED_GOMOD_VERSION}"
  echo "   The CA tag and K8S_VERSION may be mismatched. Continuing anyway..."
else
  echo "✅ Version check passed: k8s.io/api=${GOMOD_API_VERSION}"
fi

echo "🚀 Starting Proto Generation..."

# 1. Clean up Previous Builds
echo "🧹 Cleaning up..."
rm -rf "$TEMP_BUILD_DIR"
rm -rf "$DEST_DIR/k8s"
rm -rf "$DEST_DIR/clusterautoscaler"

# 2. Prepare Virtual Structure using SYMLINKS
echo "🔗 Linking package structure in $TEMP_BUILD_DIR..."
mkdir -p "$TEMP_BUILD_DIR/k8s.io"
mkdir -p "$TEMP_BUILD_DIR/$AUTOSCALER_PKG_PATH"

# Symlink the k8s folders instead of copying
ln -s "$(pwd)/$SUB_API" "$TEMP_BUILD_DIR/k8s.io/api"
ln -s "$(pwd)/$SUB_MACHINERY" "$TEMP_BUILD_DIR/k8s.io/apimachinery"

# Copy the specific Autoscaler proto
cp "$AUTOSCALER_PROTO_SRC" "$TEMP_BUILD_DIR/$AUTOSCALER_PKG_PATH/"

# 3. Identify Files to Generate
K8S_PROTOS=(
  "$TEMP_BUILD_DIR/k8s.io/api/core/v1/generated.proto"
  "$TEMP_BUILD_DIR/k8s.io/apimachinery/pkg/apis/meta/v1/generated.proto"
  "$TEMP_BUILD_DIR/k8s.io/apimachinery/pkg/api/resource/generated.proto"
  "$TEMP_BUILD_DIR/k8s.io/apimachinery/pkg/runtime/generated.proto"
  "$TEMP_BUILD_DIR/k8s.io/apimachinery/pkg/runtime/schema/generated.proto"
  "$TEMP_BUILD_DIR/k8s.io/apimachinery/pkg/util/intstr/generated.proto"
)

AUTOSCALER_TARGET="$TEMP_BUILD_DIR/$AUTOSCALER_PKG_PATH/externalgrpc.proto"

# 4. Generate Code
echo "⚙️  Generating Python code..."

python -m grpc_tools.protoc \
  -I "$TEMP_BUILD_DIR" \
  --python_out="$TEMP_BUILD_DIR" \
  --grpc_python_out="$TEMP_BUILD_DIR" \
  --pyi_out="$TEMP_BUILD_DIR" \
  "${K8S_PROTOS[@]}" \
  "$AUTOSCALER_TARGET"

# 5. Fix "k8s.io" -> "k8s/io" structure
echo "🔧 Normalizing package paths..."
if [ -d "$TEMP_BUILD_DIR/k8s.io" ]; then
    mkdir -p "$TEMP_BUILD_DIR/k8s/io"
    # Move ONLY the generated python files, ignore the source protos and other junk
    rsync -a --include="*/" --include="*.py" --include="*.pyi" --exclude="*" "$TEMP_BUILD_DIR/k8s.io/" "$TEMP_BUILD_DIR/k8s/io/"
    rm -rf "$TEMP_BUILD_DIR/k8s.io"
fi

# 6. Move ONLY generated artifacts to SRC
echo "📦 Moving generated packages to $DEST_DIR..."
mkdir -p "$DEST_DIR"

# Move k8s folder
if [ -d "$TEMP_BUILD_DIR/k8s" ]; then
    cp -r "$TEMP_BUILD_DIR/k8s" "$DEST_DIR/"
fi

# Move clusterautoscaler folder
if [ -d "$TEMP_BUILD_DIR/clusterautoscaler" ]; then
    cp -r "$TEMP_BUILD_DIR/clusterautoscaler" "$DEST_DIR/"
fi

echo "🐍 Creating __init__.py files in $DEST_DIR..."
# This will only touch directories inside src/
find "$DEST_DIR/clusterautoscaler" -type d -exec touch {}/__init__.py \;
find "$DEST_DIR/k8s" -type d -exec touch {}/__init__.py \;

# Cleanup temp
rm -rf "$TEMP_BUILD_DIR"

echo "✅ Done! Generated code is in $DEST_DIR"

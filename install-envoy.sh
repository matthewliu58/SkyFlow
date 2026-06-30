#!/bin/bash
set -euo pipefail

# --------------------------
# 1. Constants
# --------------------------
ENVOY_VERSION="1.28.0"
ENVOY_HOME="/root"
OWNER="root:root"
ENVOY_BIN="${ENVOY_HOME}/envoy"
ENVOY_CONFIG="${ENVOY_HOME}/envoy-mini.yaml"
DOWNLOAD_URL=""
PROFILE_DIR="${ENVOY_HOME}/profile"

# --------------------------
# 2. Architecture detection
# --------------------------
ARCH=$(uname -m)
if [ "$ARCH" = "x86_64" ]; then
    DOWNLOAD_URL="https://github.com/envoyproxy/envoy/releases/download/v${ENVOY_VERSION}/envoy-${ENVOY_VERSION}-linux-x86_64"
elif [ "$ARCH" = "aarch64" ]; then
    DOWNLOAD_URL="https://github.com/envoyproxy/envoy/releases/download/v${ENVOY_VERSION}/envoy-${ENVOY_VERSION}-linux-aarch64"
else
    echo "Unsupported architecture: ${ARCH}"
    exit 1
fi

# --------------------------
# 3. System dependencies
# --------------------------
sudo apt update
sudo apt install -y curl ca-certificates libssl3 --no-install-recommends
sudo apt clean

# --------------------------
# 4. Download Envoy
# --------------------------
if [ -f "${ENVOY_BIN}" ]; then
    echo "Envoy binary exists, backing up to ${ENVOY_BIN}.bak"
    mv "${ENVOY_BIN}" "${ENVOY_BIN}.bak"
fi

echo "Downloading Envoy ${ENVOY_VERSION} (${ARCH})..."
curl -L "${DOWNLOAD_URL}" -o "${ENVOY_BIN}"
chmod +x "${ENVOY_BIN}"
sudo chown "${OWNER}" "${ENVOY_BIN}"

echo "Envoy version:"
"${ENVOY_BIN}" --version

# --------------------------
# 5. Create profile directory
# --------------------------
mkdir -p "${PROFILE_DIR}"
sudo chown "${OWNER}" "${PROFILE_DIR}"
chmod 755 "${PROFILE_DIR}"

# --------------------------
# 6. Generate minimal config
# --------------------------
echo "Generating Envoy config ${ENVOY_CONFIG}..."
cat > "${ENVOY_CONFIG}" << EOF
admin:
  address:
    socket_address:
      address: 127.0.0.1
      port_value: 9901

static_resources:
  listeners: []
  clusters: []
EOF

# --------------------------
# 7. Set file permissions
# --------------------------
sudo chown "${OWNER}" "${ENVOY_CONFIG}"
chmod 644 "${ENVOY_CONFIG}"

# --------------------------
# 8. Done
# --------------------------
echo ""
echo "Envoy installed!"
echo "Key files:"
echo "  - Envoy binary: ${ENVOY_BIN}"
echo "  - Config: ${ENVOY_CONFIG}"
echo "  - Admin log: ${ENVOY_HOME}/admin_access.log"
echo ""
echo "Start command:"
echo "  ${ENVOY_BIN} -c ${ENVOY_CONFIG} &"

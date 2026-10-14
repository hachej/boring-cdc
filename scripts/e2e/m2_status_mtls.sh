#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
export TMPDIR=/var/tmp

work=$(mktemp -d /var/tmp/m2-status-mtls.XXXXXX)
cleanup() { rm -rf "$work"; }
trap cleanup EXIT INT TERM
chmod 700 "$work"

openssl req -x509 -newkey rsa:2048 -noenc -keyout "$work/ca.key" -out "$work/ca.crt" \
  -days 1 -subj '/CN=boring-cdc-test-ca' \
  -addext 'basicConstraints=critical,CA:TRUE' \
  -addext 'keyUsage=critical,keyCertSign,cRLSign' >"$work/openssl.log" 2>&1
openssl req -newkey rsa:2048 -noenc -keyout "$work/server.key" -out "$work/server.csr" \
  -subj '/CN=localhost' >>"$work/openssl.log" 2>&1
cat >"$work/server.ext" <<'EOF'
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:localhost,IP:127.0.0.1
EOF
openssl x509 -req -in "$work/server.csr" -CA "$work/ca.crt" -CAkey "$work/ca.key" \
  -CAcreateserial -out "$work/server.crt" -days 1 -extfile "$work/server.ext" \
  >>"$work/openssl.log" 2>&1
openssl req -newkey rsa:2048 -noenc -keyout "$work/client.key" -out "$work/client.csr" \
  -subj '/CN=boring-cdc-test-client' >>"$work/openssl.log" 2>&1
cat >"$work/client.ext" <<'EOF'
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature
extendedKeyUsage=clientAuth
EOF
openssl x509 -req -in "$work/client.csr" -CA "$work/ca.crt" -CAkey "$work/ca.key" \
  -out "$work/client.crt" -days 1 -extfile "$work/client.ext" \
  >>"$work/openssl.log" 2>&1
openssl req -x509 -newkey rsa:2048 -noenc -keyout "$work/other-ca.key" -out "$work/other-ca.crt" \
  -days 1 -subj '/CN=boring-cdc-untrusted-ca' \
  -addext 'basicConstraints=critical,CA:TRUE' \
  -addext 'keyUsage=critical,keyCertSign,cRLSign' >>"$work/openssl.log" 2>&1
openssl req -newkey rsa:2048 -noenc -keyout "$work/outsider.key" -out "$work/outsider.csr" \
  -subj '/CN=boring-cdc-untrusted-client' >>"$work/openssl.log" 2>&1
openssl x509 -req -in "$work/outsider.csr" -CA "$work/other-ca.crt" -CAkey "$work/other-ca.key" \
  -CAcreateserial -out "$work/outsider.crt" -days 1 -extfile "$work/client.ext" \
  >>"$work/openssl.log" 2>&1
chmod 600 "$work"/*.key

export BCDC_TEST_STATUS_SERVER_CERT_FILE="$work/server.crt"
export BCDC_TEST_STATUS_SERVER_KEY_FILE="$work/server.key"
export BCDC_TEST_STATUS_CA_FILE="$work/ca.crt"
export BCDC_TEST_STATUS_CLIENT_CERT_FILE="$work/client.crt"
export BCDC_TEST_STATUS_CLIENT_KEY_FILE="$work/client.key"
export BCDC_TEST_STATUS_UNTRUSTED_CERT_FILE="$work/outsider.crt"
export BCDC_TEST_STATUS_UNTRUSTED_KEY_FILE="$work/outsider.key"
cargo test --locked --lib \
  m2_status_listener::tests::mtls_rejects_anonymous_client_and_serves_authenticated_status \
  -- --ignored --exact
printf 'M2_STATUS_MTLS_OK authenticated=verified anonymous=rejected material=ephemeral\n'

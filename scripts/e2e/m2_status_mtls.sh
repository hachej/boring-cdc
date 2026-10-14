#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
export TMPDIR=/var/tmp

work=$(mktemp -d /var/tmp/m2-status-mtls.XXXXXX)
acceptance_pid=
cleanup() {
  [[ -z "$acceptance_pid" ]] || { kill "$acceptance_pid" >/dev/null 2>&1 || true; wait "$acceptance_pid" >/dev/null 2>&1 || true; }
  rm -rf "$work"
}
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

python3 - "$work/fixture.toml" <<'PY'
from pathlib import Path
import sys

fixture = Path('tests/fixtures/m1_config/representative.toml').read_text()
changes = {
    'status_listen_addr = "127.0.0.1:8787"': 'status_listen_addr = "0.0.0.0:8787"',
    'prometheus_listen_addr = "127.0.0.1:8788"': 'prometheus_listen_addr = "0.0.0.0:8788"',
    'authentication = false': 'authentication = true',
    'tls = false': 'tls = true',
}
for before, after in changes.items():
    assert fixture.count(before) == 1, before
    fixture = fixture.replace(before, after, 1)
Path(sys.argv[1]).write_text(fixture)
PY
export STATUS_TLS_CERT STATUS_TLS_KEY STATUS_TLS_CLIENT_CA
STATUS_TLS_CERT=$(cat "$work/server.crt")
STATUS_TLS_KEY=$(cat "$work/server.key")
STATUS_TLS_CLIENT_CA=$(cat "$work/ca.crt")
M2_STATUS_FIXTURE_CONFIG="$work/fixture.toml" scripts/acceptance/durable_simple_case.sh >"$work/acceptance.log" 2>&1 &
acceptance_pid=$!
observed=0
for _ in {1..200}; do
  if python3 - "$work/ca.crt" "$work/client.crt" "$work/client.key" 2>/dev/null <<'PY'
import json, ssl, sys, urllib.error, urllib.request

ca, cert, key = sys.argv[1:]
trusted = ssl.create_default_context(cafile=ca)
trusted.load_cert_chain(certfile=cert, keyfile=key)
anonymous = ssl.create_default_context(cafile=ca)
for port, route in ((8787, 'status'), (8788, 'metrics')):
    url = f'https://127.0.0.1:{port}/{route}'
    with urllib.request.urlopen(url, context=trusted, timeout=1) as response:
        assert response.status == 200
        body = response.read()
    if route == 'status':
        assert json.loads(body)['command'] == 'CMD-STATUS'
    else:
        assert b'boring_cdc_status_available 1\n' in body
    try:
        urllib.request.urlopen(urllib.request.Request(url, method='POST'), context=trusted, timeout=1)
    except urllib.error.HTTPError as error:
        assert error.code == 405
    else:
        raise AssertionError('mutating method accepted')
    try:
        urllib.request.urlopen(url, context=anonymous, timeout=1)
    except (urllib.error.URLError, OSError):
        pass
    else:
        raise AssertionError('anonymous client received a response')
PY
  then
    observed=1
    break
  fi
  kill -0 "$acceptance_pid" >/dev/null 2>&1 || break
  sleep .2
done
if ! wait "$acceptance_pid"; then
  cat "$work/acceptance.log" >&2
  exit 1
fi
acceptance_pid=
[[ "$observed" == 1 ]]
grep -q '^DURABLE_SIMPLE_CASE_OK ' "$work/acceptance.log"
printf 'M2_STATUS_MTLS_OK postgres=17.6 live_capture=verified authenticated=verified anonymous=rejected status=read_only metrics=read_only material=ephemeral\n'

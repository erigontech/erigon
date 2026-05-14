#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENCLAVE_NAME="${ENCLAVE_NAME:-ssz-rest-erigon}"
ERIGON_IMAGE="${ERIGON_IMAGE:-local-erigon:ssz-rest}"
PRYSM_IMAGE="${PRYSM_IMAGE:-local-prysm:ssz-rest}"
PRYSM_VALIDATOR_IMAGE="${PRYSM_VALIDATOR_IMAGE:-local-prysm-validator:ssz-rest}"
SPAMOOR_IMAGE="${SPAMOOR_IMAGE:-ethpandaops/spamoor:latest}"
PARAMS_FILE="${PARAMS_FILE:-/tmp/${ENCLAVE_NAME}-network-params.yaml}"
ETHEREUM_PACKAGE="${ETHEREUM_PACKAGE:-github.com/ethpandaops/ethereum-package@87df9d9d15e493ea91db788161e54324c39f6a6e}"

cd "${ROOT_DIR}"

docker build -t "${ERIGON_IMAGE}" .

cat > "${PARAMS_FILE}" <<YAML
participants:
  - el_type: erigon
    el_image: ${ERIGON_IMAGE}
    el_extra_params:
      - --authrpc.addr=0.0.0.0
      - --authrpc.port=8551
      - --authrpc.vhosts=*
      - --http
      - --http.addr=0.0.0.0
      - --http.api=eth,net,web3,erigon,admin
    cl_type: prysm
    cl_image: ${PRYSM_IMAGE}
    cl_extra_params:
      - --enable-ssz-rest-engine-api
    vc_image: ${PRYSM_VALIDATOR_IMAGE}
network_params:
  seconds_per_slot: 4
  deneb_fork_epoch: 0
  electra_fork_epoch: 0
additional_services:
  - spamoor
spamoor_params:
  image: ${SPAMOOR_IMAGE}
  spammers:
    - scenario: eoatx
      name: ssz-rest-eoatx
      config:
        throughput: 25
        max_pending: 100
        max_wallets: 50
  extra_args: []
YAML

kurtosis enclave rm -f "${ENCLAVE_NAME}" >/dev/null 2>&1 || true
kurtosis run "${ETHEREUM_PACKAGE}" --enclave "${ENCLAVE_NAME}" --args-file "${PARAMS_FILE}"

cat <<EOF
Enclave: ${ENCLAVE_NAME}
Erigon logs:  kurtosis service logs ${ENCLAVE_NAME} el-1-erigon-prysm
Prysm logs:   kurtosis service logs ${ENCLAVE_NAME} cl-1-prysm-erigon
Spamoor logs: kurtosis service logs ${ENCLAVE_NAME} spamoor

Verify 4 second slots and SSZ-REST calls:
  kurtosis enclave inspect ${ENCLAVE_NAME}
  kurtosis service logs ${ENCLAVE_NAME} el-1-erigon-prysm | grep -E '\\[SSZ-REST\\]|/engine/v.*/(payloads|forkchoice|blobs|capabilities)'
  kurtosis service logs ${ENCLAVE_NAME} cl-1-prysm-erigon | grep -E 'finalized|POST /engine|GET /engine|SSZ'
EOF

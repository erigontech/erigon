#!/bin/bash

mkdir -p /ansible/group_vars

cat <<EOL > /ansible/group_vars/all.yml
---
rpc_url: "${RPC_URL}"
private_key: "${PRIVATE_KEY}"
eth_address: "${ETH_ADDRESS}"
log_file: ${LOG_FILE}
work_dir: ${WORK_DIR}
max_block_size: ${MAX_BLOCK_SIZE}
legacy_flag: "${LEGACY_FLAG}"
block_interval: ${BLOCK_INTERVAL}
EOL

ansible-playbook /ansible/site.yml $extra_vars
set -a # automatically export all variables
source .env
set +a

if [ -z "$rpc_url" ]; then
    echo "Please fill "rpc_url" in the .env file"
    exit 1
fi

if [ -z "$private_key" ]; then
    echo "Please fill "private_key" in the .env file"
    exit 1
fi

addr=$(cast wallet address --private-key $private_key)
nonce=$(cast nonce --rpc-url $rpc_url $addr)

echo "Test address: $addr (nonce: $nonce) balance $(cast balance --rpc-url $rpc_url $addr)"
cast send --legacy --rpc-url $rpc_url --private-key $private_key --async --create 0x5b3456 
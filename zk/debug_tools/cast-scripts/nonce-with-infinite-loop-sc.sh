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
cast send --legacy --rpc-url $rpc_url --private-key $private_key --gas-limit 5000000000000000000 --gas-price 1100 --async --create 0x5b3456 
echo "Test address: $addr (nonce: $(cast nonce --rpc-url $rpc_url $addr)) balance $(cast balance --rpc-url $rpc_url $addr)"
#sleep 5 seconds
echo "Sleeping for 5 seconds..."
sleep 5

echo "Test address: $addr (nonce: $(cast nonce --rpc-url $rpc_url $addr)) balance $(cast balance --rpc-url $rpc_url $addr)"
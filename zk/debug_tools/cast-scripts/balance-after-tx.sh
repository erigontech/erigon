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
# balance before sending funds
echo "Balance before sending funds $(cast balance --rpc-url $rpc_url 0x5e8f0f2f8b364e2f0f3f1f1f1f1f1f1f1f1f1f1f)"

# send some funds
cast send --rpc-url $rpc_url --legacy --value 0.1ether --private-key $private_key 0x5e8f0f2f8b364e2f0f3f1f1f1f1f1f1f1f1f1f1f

# now get the balance of the accounts
echo "Balance after sending funds $(cast balance --rpc-url $rpc_url 0x5e8f0f2f8b364e2f0f3f1f1f1f1f1f1f1f1f1f1f)"

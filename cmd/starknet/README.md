# How to deploy cairo smart contract

1. Compile cairo smart contract

    `starknet-compile contract.cairo     --output contract_compiled.json     --abi contract_abi.json`


2. Generate payload for `starknet_sendRawTransaction` PRC method

    ```
   go run ./cmd/starknet/main.go generateRawTx 
       -c ./cairo/contract.json 
       -o /cairo/send_raw_transaction 
       -s salt_test 
       -g 11452296 
       -k b9a8b19ff082a7f4b943fcbe0da6cce6ce2c860090f05d031f463412ab534e95
   ```

    Command syntax: `go run main.go generateRawTx --help`   


3. Use command output in RPC call

```json
"params":["0x03f86583127ed80180800180019637623232363136323639323233613230356235643764c080a0b44c2f4e18ca27e621171da5cf3a0c875c0749c7b998ec2759974280d987143aa04f01823122d972baa1a03b113535d9f9057fd9366fd8770e766b91f835b88ea6"],
```

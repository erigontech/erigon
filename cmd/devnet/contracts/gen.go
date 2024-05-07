package contracts

// rootsender.sol
//go:generate solc --evm-version paris --allow-paths ., --abi --bin --overwrite --optimize -o build rootsender.sol
//go:generate abigen -abi build/RootSender.abi -bin build/RootSender.bin -pkg contracts -type RootSender -out ./gen_rootsender.go

// childsender.sol
//go:generate solc --evm-version paris --allow-paths ., --abi --bin --overwrite --optimize -o build childsender.sol
//go:generate abigen -abi build/ChildSender.abi -bin build/ChildSender.bin -pkg contracts -type ChildSender -out ./gen_childsender.go

// teststatesender.sol
//go:generate solc --evm-version paris --allow-paths ., --abi --bin --overwrite --optimize -o build teststatesender.sol
//go:generate abigen -abi build/TestStateSender.abi -bin build/TestStateSender.bin -pkg contracts -type TestStateSender -out ./gen_teststatesender.go

// rootreceiver.sol
//go:generate solc --evm-version paris --allow-paths ., --abi --bin --overwrite --optimize -o build rootreceiver.sol
//go:generate abigen -abi build/RootReceiver.abi -bin build/RootReceiver.bin -pkg contracts -type RootReceiver -out ./gen_rootreceiver.go

// childreceiver.sol
//go:generate solc --evm-version paris --allow-paths ., --abi --bin --overwrite --optimize -o build childreceiver.sol
//go:generate abigen -abi build/ChildReceiver.abi -bin build/ChildReceiver.bin -pkg contracts -type ChildReceiver -out ./gen_childreceiver.go

// testrootchain.sol
//go:generate solc --evm-version paris --allow-paths ., --abi --bin --overwrite --optimize -o build testrootchain.sol
//go:generate abigen -abi build/TestRootChain.abi -bin build/TestRootChain.bin -pkg contracts -type TestRootChain -out ./gen_testrootchain.go

// faucet.sol
//go:generate solc --evm-version paris --allow-paths ., --abi --bin --overwrite --optimize -o build faucet.sol
//go:generate abigen -abi build/Faucet.abi -bin build/Faucet.bin -pkg contracts -type Faucet -out ./gen_faucet.go

package contracts

// rootsender.sol
//go:generate solc --evm-version paris --allow-paths ., --abi --bin --overwrite --optimize -o build rootsender.sol
//go:generate abigen -abi build/RootSender.abi -bin build/RootSender.bin -pkg contracts -type RootSender -out ./gen_rootsender.go

// teststatesender.sol
//go:generate solc --evm-version paris --allow-paths ., --abi --bin --overwrite --optimize -o build teststatesender.sol
//go:generate abigen -abi build/TestStateSender.abi -bin build/TestStateSender.bin -pkg contracts -type TestStateSender -out ./gen_teststatesender.go

// childreceiver.sol
//go:generate solc --evm-version paris --allow-paths ., --abi --bin --overwrite --optimize -o build childreceiver.sol
//go:generate abigen -abi build/ChildReceiver.abi -bin build/ChildReceiver.bin -pkg contracts -type ChildReceiver -out ./gen_childreceiver.go

// faucet.sol
//go:generate solc --evm-version paris --allow-paths ., --abi --bin --overwrite --optimize -o build faucet.sol
//go:generate abigen -abi build/Faucet.abi -bin build/Faucet.bin -pkg contracts -type Faucet -out ./gen_faucet.go

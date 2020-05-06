package contracts

//go:generate solc --allow-paths ., --abi --bin --overwrite -o build testcontract.sol
//go:generate abigen -abi build/testcontract.abi -bin build/testcontract.bin -pkg contracts -type testcontract -out ./gen_testcontract.go

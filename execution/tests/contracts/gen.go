package contracts

// testcontract.sol
//go:generate solc --allow-paths ., --abi --bin --overwrite --optimize -o build testcontract.sol
//go:generate abigen -abi build/testcontract.abi -bin build/testcontract.bin -pkg contracts -type testcontract -out ./gen_testcontract.go

// selfDestructor.sol
//go:generate solc --allow-paths ., --abi --bin --overwrite --optimize -o build selfDestructor.sol
//go:generate abigen -abi build/selfDestructor.abi -bin build/selfDestructor.bin -pkg contracts -type selfDestructor -out ./gen_selfDestructor.go

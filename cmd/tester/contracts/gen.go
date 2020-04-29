package contracts

// revive2.sol
//go:generate solc --allow-paths ., --abi --bin --overwrite --optimize -o build revive2.sol
//go:generate abigen -abi build/Revive2.abi -bin build/Revive2.bin -pkg contracts -type revive2 -out ./gen_revive2.go
//go:generate abigen -abi build/Phoenix.abi -bin build/Phoenix.bin -pkg contracts -type phoenix -out ./gen_phoenix.go

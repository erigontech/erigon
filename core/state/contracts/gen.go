package contracts

// changer.sol
//go:generate solc --allow-paths ., --abi --bin --overwrite --optimize -o build changer.sol
//go:generate abigen -abi build/Changer.abi -bin build/Changer.bin -pkg contracts -type changer -out ./gen_changer.go

// revive.sol
//go:generate solc --allow-paths ., --abi --bin --overwrite --optimize -o build changer.sol
//go:generate abigen -abi build/Changer.abi -bin build/Changer.bin -pkg contracts -type changer -out ./gen_changer.go

// selfdestruct.sol
//go:generate solc --allow-paths ., --abi --bin --overwrite --optimize -o build revive.sol
//go:generate abigen -abi build/Revive.abi -bin build/Revive.bin -pkg contracts -type revive -out ./gen_revive.go

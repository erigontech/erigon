package contracts

// changer.sol
//go:generate solc --allow-paths ., --abi --bin --overwrite --optimize -o build changer.sol
//go:generate abigen -abi build/Changer.abi -bin build/Changer.bin -pkg contracts -type changer -out ./gen_changer.go

// revive.sol
//go:generate solc --allow-paths ., --abi --bin --overwrite --optimize -o build revive.sol
//go:generate abigen -abi build/Revive.abi -bin build/Revive.bin -pkg contracts -type revive -out ./gen_revive.go

// selfdestruct.sol
//go:generate solc --allow-paths ., --abi --bin --overwrite --optimize -o build selfdestruct.sol
//go:generate abigen -abi build/Selfdestruct.abi -bin build/Selfdestruct.bin -pkg contracts -type selfdestruct -out ./gen_selfdestruct.go

// poly.sol
//go:generate solc --allow-paths ., --abi --bin --overwrite --optimize -o build poly.sol
//go:generate abigen -abi build/Poly.abi -bin build/Poly.bin -pkg contracts -type poly -out ./gen_poly.go

package contracts

//go:generate solc --allow-paths ., --abi --bin --overwrite --optimize -o build token.sol
//go:generate abigen -abi build/Token.abi -bin build/Token.bin -pkg contracts -type token -out ./gen_token.go

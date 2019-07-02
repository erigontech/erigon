package contracts

//go:generate solc --allow-paths ., --abi --bin --overwrite -o build eip2027.sol
//go:generate abigen -abi build/eip2027.abi -bin build/eip2027.bin -pkg contracts -type eip2027 -out ./gen_eip2027.go

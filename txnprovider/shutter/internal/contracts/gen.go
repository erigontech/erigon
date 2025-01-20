package contracts

//go:generate solc --allow-paths ., --abi --bin --overwrite --optimize -o build Sequencer.sol
//go:generate abigen -abi build/Sequencer.abi -bin build/Sequencer.bin -pkg contracts -type sequencer -out ./gen_sequencer.go

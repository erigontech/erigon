package contracts

//go:generate solc --allow-paths ., --abi --bin --overwrite --optimize -o build Sequencer.sol
//go:generate abigen -abi build/Sequencer.abi -bin build/Sequencer.bin -pkg contracts -type sequencer -out ./gen_sequencer.go

//go:generate solc --allow-paths ., --abi --bin --overwrite --optimize -o build KeyperSet.sol
//go:generate abigen -abi build/KeyperSet.abi -bin build/KeyperSet.bin -pkg contracts -type keyperSet -out ./gen_keyper_set.go

//go:generate solc --allow-paths ., --abi --bin --overwrite --optimize -o build KeyperSetManager.sol
//go:generate abigen -abi build/KeyperSetManager.abi -bin build/KeyperSetManager.bin -pkg contracts -type keyperSetManager -out ./gen_keyper_set_manager.go

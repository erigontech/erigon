// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package contracts

// rootsender.sol
//go:generate solc --evm-version paris --allow-paths ., --abi --bin --overwrite --optimize -o build rootsender.sol
//go:generate abigen -abi build/RootSender.abi -bin build/RootSender.bin -pkg contracts -type RootSender -out ./gen_rootsender.go

// childsender.sol
//go:generate solc --evm-version paris --allow-paths ., --abi --bin --overwrite --optimize -o build childsender.sol
//go:generate abigen -abi build/ChildSender.abi -bin build/ChildSender.bin -pkg contracts -type ChildSender -out ./gen_childsender.go

// teststatesender.sol
//go:generate solc --evm-version paris --allow-paths ., --abi --bin --overwrite --optimize -o build teststatesender.sol
//go:generate abigen -abi build/TestStateSender.abi -bin build/TestStateSender.bin -pkg contracts -type TestStateSender -out ./gen_teststatesender.go

// rootreceiver.sol
//go:generate solc --evm-version paris --allow-paths ., --abi --bin --overwrite --optimize -o build rootreceiver.sol
//go:generate abigen -abi build/RootReceiver.abi -bin build/RootReceiver.bin -pkg contracts -type RootReceiver -out ./gen_rootreceiver.go

// childreceiver.sol
//go:generate solc --evm-version paris --allow-paths ., --abi --bin --overwrite --optimize -o build childreceiver.sol
//go:generate abigen -abi build/ChildReceiver.abi -bin build/ChildReceiver.bin -pkg contracts -type ChildReceiver -out ./gen_childreceiver.go

// testrootchain.sol
//go:generate solc --evm-version paris --allow-paths ., --abi --bin --overwrite --optimize -o build testrootchain.sol
//go:generate abigen -abi build/TestRootChain.abi -bin build/TestRootChain.bin -pkg contracts -type TestRootChain -out ./gen_testrootchain.go

// faucet.sol
//go:generate solc --evm-version paris --allow-paths ., --abi --bin --overwrite --optimize -o build faucet.sol
//go:generate abigen -abi build/Faucet.abi -bin build/Faucet.bin -pkg contracts -type Faucet -out ./gen_faucet.go

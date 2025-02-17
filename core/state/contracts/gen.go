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

// revive2.sol
//go:generate solc --allow-paths ., --abi --bin --overwrite --optimize -o build revive2.sol
//go:generate abigen -abi build/Revive2.abi -bin build/Revive2.bin -pkg contracts -type revive2 -out ./gen_revive2.go
//go:generate abigen -abi build/Phoenix.abi -bin build/Phoenix.bin -pkg contracts -type phoenix -out ./gen_phoenix.go

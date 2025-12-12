// Copyright 2016 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package osver

const (
	ArbosVersion_0  = uint64(0)
	ArbosVersion_1  = uint64(1)
	ArbosVersion_2  = uint64(2)
	ArbosVersion_3  = uint64(3)
	ArbosVersion_4  = uint64(4)
	ArbosVersion_5  = uint64(5)
	ArbosVersion_6  = uint64(6)
	ArbosVersion_7  = uint64(7)
	ArbosVersion_8  = uint64(8)
	ArbosVersion_9  = uint64(9)
	ArbosVersion_10 = uint64(10)
	ArbosVersion_11 = uint64(11)
	ArbosVersion_20 = uint64(20)
	ArbosVersion_30 = uint64(30)
	ArbosVersion_31 = uint64(31)
	ArbosVersion_32 = uint64(32)
	ArbosVersion_40 = uint64(40)
	ArbosVersion_41 = uint64(41)
	ArbosVersion_50 = uint64(50)
	ArbosVersion_51 = uint64(51)
)

const ArbosVersion_FixRedeemGas = ArbosVersion_11
const ArbosVersion_Stylus = ArbosVersion_30
const ArbosVersion_StylusFixes = ArbosVersion_31
const ArbosVersion_StylusChargingFixes = ArbosVersion_32
const MaxArbosVersionSupported = ArbosVersion_51
const MaxDebugArbosVersionSupported = ArbosVersion_51
const ArbosVersion_Dia = ArbosVersion_50

const ArbosVersion_MultiConstraintFix = ArbosVersion_51

func IsStylus(arbosVersion uint64) bool {
	return arbosVersion >= ArbosVersion_Stylus
}

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

package engine_helpers

import "github.com/erigontech/erigon/rpc"

const MaxBuilders = 128

// MaxAncestorReorgDepth is the maximum depth behind the current EL head for which
// a reorg to a canonical ancestor MUST be supported on FCU. See
// ethereum/execution-apis#770: within this depth we always reorg; beyond it we
// MAY skip the forkchoice state update and return VALID with payloadId = null.
const MaxAncestorReorgDepth = 32

var UnknownPayloadErr = rpc.CustomError{Code: -38001, Message: "Unknown payload"}
var InvalidForkchoiceStateErr = rpc.CustomError{Code: -38002, Message: "Invalid forkchoice state"}
var InvalidPayloadAttributesErr = rpc.CustomError{Code: -38003, Message: "Invalid payload attributes"}
var TooLargeRequestErr = rpc.CustomError{Code: -38004, Message: "Too large request"}

const PectraBanner = `
'########::'########::'######::'########:'########:::::'###::::
 ##.... ##: ##.....::'##... ##:... ##..:: ##.... ##:::'## ##:::
 ##:::: ##: ##::::::: ##:::..::::: ##:::: ##:::: ##::'##:. ##::
 ########:: ######::: ##:::::::::: ##:::: ########::'##:::. ##:
 ##.....::: ##...:::: ##:::::::::: ##:::: ##.. ##::: #########:
 ##:::::::: ##::::::: ##::: ##:::: ##:::: ##::. ##:: ##.... ##:
 ##:::::::: ########:. ######::::: ##:::: ##:::. ##: ##:::: ##:
..:::::::::........:::......::::::..:::::..:::::..::..:::::..::
====================== PECTRA ACTIVATED ======================
`

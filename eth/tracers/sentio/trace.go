package sentio

import (
	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/common/math"
)

type Trace struct {
	//op      vm.OpCode
	Type    string              `json:"type"`
	Pc      uint64              `json:"pc"`
	Index   int                 `json:"index"`
	GasIn   math.HexOrDecimal64 `json:"gasIn"` // TODO this should be hex
	Gas     math.HexOrDecimal64 `json:"gas"`
	GasCost math.HexOrDecimal64 `json:"gasCost"`
	GasUsed math.HexOrDecimal64 `json:"gasUsed"`
	Output  hexutil.Bytes       `json:"output,omitempty"`
	From    *libcommon.Address  `json:"from,omitempty"`

	// Used by call
	To          *libcommon.Address `json:"to,omitempty"`
	Input       string             `json:"input,omitempty"` // TODO better struct it and make it bytes
	Value       hexutil.Bytes      `json:"value"`
	ErrorString string             `json:"error,omitempty"`

	// Used by jump
	Stack []uint256.Int `json:"stack,omitempty"`
	//Stack  [][4]uint64 `json:"stack,omitempty"`
	//Memory []byte `json:"memory,omitempty"`
	Memory *[]string `json:"memory,omitempty"`

	// Used by log
	Address *libcommon.Address `json:"address,omitempty"`
	Data    hexutil.Bytes      `json:"data,omitempty"`
	Topics  []hexutil.Bytes    `json:"topics,omitempty"`

	// Only used by root
	Traces []Trace `json:"traces,omitempty"`
}

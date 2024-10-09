package engine_helpers

import "github.com/ledgerwatch/erigon/rpc"

const MaxBuilders = 128

var UnknownPayloadErr = rpc.CustomError{Code: -38001, Message: "Unknown payload"}
var InvalidForkchoiceStateErr = rpc.CustomError{Code: -38002, Message: "Invalid forkchoice state"}
var InvalidPayloadAttributesErr = rpc.CustomError{Code: -38003, Message: "Invalid payload attributes"}
var TooLargeRequestErr = rpc.CustomError{Code: -38004, Message: "Too large request"}

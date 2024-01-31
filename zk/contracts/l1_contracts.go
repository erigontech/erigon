package contracts

import (
	"github.com/ledgerwatch/erigon-lib/common"
)

var (
	SequencedBatchTopic         = common.HexToHash("0x303446e6a8cb73c83dff421c0b1d5e5ce0719dab1bff13660fc254e58cc17fce")
	VerificationTopic           = common.HexToHash("0xcb339b570a7f0b25afa7333371ff11192092a0aeace12b671f4c212f2815c6fe")
	UpdateL1InfoTreeTopic       = common.HexToHash("0xda61aa7823fcd807e37b95aabcbe17f03a6f3efd514176444dae191d27fd66b3")
	InitialSequenceBatchesTopic = common.HexToHash("0x060116213bcbf54ca19fd649dc84b59ab2bbd200ab199770e4d923e222a28e7f")
)

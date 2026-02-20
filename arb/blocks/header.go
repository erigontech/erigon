package arbBlocks

import (
	"encoding/binary"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
)

type HeaderInfo struct {
	SendRoot           common.Hash
	SendCount          uint64
	L1BlockNumber      uint64
	ArbOSFormatVersion uint64
}

func (info HeaderInfo) extra() []byte {
	return info.SendRoot[:]
}

func (info HeaderInfo) mixDigest() [32]byte {
	mixDigest := common.Hash{}
	binary.BigEndian.PutUint64(mixDigest[:8], info.SendCount)
	binary.BigEndian.PutUint64(mixDigest[8:16], info.L1BlockNumber)
	binary.BigEndian.PutUint64(mixDigest[16:24], info.ArbOSFormatVersion)
	return mixDigest
}

func (info HeaderInfo) UpdateHeaderWithInfo(header *types.Header) {
	header.MixDigest = info.mixDigest()
	header.Extra = info.extra()
}

func DeserializeHeaderExtraInformation(header *types.Header) HeaderInfo {
	if header == nil || header.BaseFee == nil || header.BaseFee.Sign() == 0 || len(header.Extra) != 32 || header.Difficulty.Cmp(common.Big1) != 0 {
		// imported blocks have no base fee
		// The genesis block doesn't have an ArbOS encoded extra field
		return HeaderInfo{}
	}
	extra := HeaderInfo{}
	copy(extra.SendRoot[:], header.Extra)
	extra.SendCount = binary.BigEndian.Uint64(header.MixDigest[:8])
	extra.L1BlockNumber = binary.BigEndian.Uint64(header.MixDigest[8:16])
	extra.ArbOSFormatVersion = binary.BigEndian.Uint64(header.MixDigest[16:24])
	return extra
}

func GetArbOSVersion(header *types.Header, chain *chain.Config) uint64 {
	if !chain.IsArbitrum() {
		return 0
	}
	extraInfo := DeserializeHeaderExtraInformation(header)
	return extraInfo.ArbOSFormatVersion
}

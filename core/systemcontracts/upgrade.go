package systemcontracts

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

var (
	// SystemContractCodeLookup is used to address a flaw in the upgrade logic of the system contracts. Since they are updated directly, without first being self-destructed
	// and then re-created, the usual incarnation logic does not get activated, and all historical records of the code of these contracts are retrieved as the most
	// recent version. This problem will not exist in erigon3, but until then, a workaround will be used to access code of such contracts through this structure
	// Lookup is performed first by chain name, then by contract address. The value in the map is the list of CodeRecords, with increasing block numbers,
	// to be used in binary search to determine correct historical code
	SystemContractCodeLookup = map[string]map[libcommon.Address][]libcommon.CodeRecord{}
)

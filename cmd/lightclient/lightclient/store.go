package lightclient

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
)

type LightClientStore struct {
	// Beacon block header that is finalized
	finalizedHeader *cltypes.BeaconBlockHeader
	// Most recent available reasonably-safe header
	optimisticHeader *cltypes.BeaconBlockHeader

	// Sync committees corresponding to the header
	currentSyncCommittee *cltypes.SyncCommittee
	nextSyncCommittee    *cltypes.SyncCommittee

	// Best available header to switch finalized head to if we see nothing else
	bestValidUpdate *cltypes.LightClientUpdate

	// Max number of active participants in a sync committee (used to calculate safety threshold)
	previousMaxActivePartecipants uint64
	currentMaxActivePartecipants  uint64
}

/*
 *	A light client maintains its state in a store object of type LightClientStore.
 *	initialize_light_client_store initializes a new store with a
 *	received LightClientBootstrap derived from a given trusted_block_root.
 */
func NewLightClientStore(trustedRoot [32]byte, bootstrap *cltypes.LightClientBootstrap) (*LightClientStore, error) {
	headerRoot, err := bootstrap.Header.HeaderEth2.HashSSZ()
	if err != nil {
		return nil, err
	}
	if headerRoot != trustedRoot {
		return nil, fmt.Errorf("trusted root is mismatching, headerRoot: %x, trustedRoot: %x",
			headerRoot, trustedRoot)
	}

	syncCommitteeRoot, err := bootstrap.CurrentSyncCommittee.HashSSZ()
	if err != nil {
		return nil, err
	}
	if !utils.IsValidMerkleBranch(
		syncCommitteeRoot,
		bootstrap.CurrentSyncCommitteeBranch,
		5,  // floorlog2(CURRENT_SYNC_COMMITTEE_INDEX)
		22, // get_subtree_index(CURRENT_SYNC_COMMITTEE_INDEX),
		bootstrap.Header.HeaderEth2.Root,
	) {
		return nil, fmt.Errorf("invalid sync committee")
	}

	return &LightClientStore{
		finalizedHeader:               bootstrap.Header.HeaderEth2,
		currentSyncCommittee:          bootstrap.CurrentSyncCommittee,
		nextSyncCommittee:             nil,
		optimisticHeader:              bootstrap.Header.HeaderEth2,
		previousMaxActivePartecipants: 0,
		currentMaxActivePartecipants:  0,
	}, nil
}

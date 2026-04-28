package raw

import (
	"fmt"
	"os"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/merkle_tree"
)

func TestGenesisStateHash(t *testing.T) {
	data, err := os.ReadFile("/tmp/kurtosis_genesis/genesis.ssz")
	if err != nil {
		t.Skipf("Genesis state file not found: %v", err)
	}

	beaconCfg, _, err := clparams.CustomConfig("/tmp/kurtosis_genesis/config.yaml")
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	version := beaconCfg.GetCurrentStateVersion(0)
	t.Logf("State version at epoch 0: %d (%s)", version, version.String())

	st := New(&beaconCfg)
	if err := st.DecodeSSZ(data, int(version)); err != nil {
		t.Fatalf("DecodeSSZ failed: %v", err)
	}
	t.Logf("Decoded: slot=%d, version=%d", st.slot, st.version)
	t.Logf("BodyRoot = %x", st.latestBlockHeader.BodyRoot)

	// Compute full hash
	stateRoot, err := st.HashSSZ()
	if err != nil {
		t.Fatalf("HashSSZ failed: %v", err)
	}
	t.Logf("Caplin state root: %x", stateRoot)
	t.Logf("Expected (LH):    99de45a86437daaa867375c67da981cd6a651c6c41d61c9a0eaf8c2afb06ae6d")

	// Round-trip check
	encoded, err := st.EncodeSSZ(nil)
	if err != nil {
		t.Fatalf("EncodeSSZ failed: %v", err)
	}
	if len(data) != len(encoded) {
		t.Errorf("SSZ length mismatch: orig=%d, enc=%d", len(data), len(encoded))
	} else {
		diff := false
		for i := 0; i < len(data); i++ {
			if data[i] != encoded[i] {
				t.Errorf("SSZ byte diff at %d: orig=0x%02x enc=0x%02x", i, data[i], encoded[i])
				diff = true
				break
			}
		}
		if !diff {
			t.Logf("SSZ round-trip: PERFECT (all %d bytes match)", len(data))
		}
	}

	// Now hash each field individually to find divergence
	schema := st.getSchema()
	t.Logf("Schema has %d fields", len(schema))
	fieldNames := getFieldNames(int(version))
	for i, field := range schema {
		h, err := merkle_tree.HashTreeRoot(field)
		name := fmt.Sprintf("field_%d", i)
		if i < len(fieldNames) {
			name = fieldNames[i]
		}
		if err != nil {
			t.Logf("  [%d] %s: HashSSZ error: %v", i, name, err)
		} else {
			t.Logf("  [%d] %s: %x", i, name, h)
		}
	}

	// Check if the hash would be correct with one fewer field (tree structure issue?)
	for n := len(schema); n >= len(schema)-2; n-- {
		h, err := merkle_tree.HashTreeRoot(schema[:n]...)
		if err != nil {
			t.Logf("Hash with %d fields: error: %v", n, err)
		} else {
			t.Logf("Hash with %d fields: %x", n, h)
		}
	}
}

func getFieldNames(version int) []string {
	names := []string{
		"genesis_time",                  // 0
		"genesis_validators_root",       // 1
		"slot",                          // 2
		"fork",                          // 3
		"latest_block_header",           // 4
		"block_roots",                   // 5
		"state_roots",                   // 6
		"historical_roots",              // 7
		"eth1_data",                     // 8
		"eth1_data_votes",               // 9
		"eth1_deposit_index",            // 10
		"validators",                    // 11
		"balances",                      // 12
		"randao_mixes",                  // 13
		"slashings",                     // 14
		"previous_epoch_participation",  // 15
		"current_epoch_participation",   // 16
		"justification_bits",            // 17
		"previous_justified_checkpoint", // 18
		"current_justified_checkpoint",  // 19
		"finalized_checkpoint",          // 20
		"inactivity_scores",             // 21
		"current_sync_committee",        // 22
		"next_sync_committee",           // 23
	}
	if version >= int(clparams.BellatrixVersion) {
		names = append(names, "latest_execution_payload_header") // 24
	}
	if version >= int(clparams.CapellaVersion) {
		names = append(names,
			"next_withdrawal_index",           // 25
			"next_withdrawal_validator_index",  // 26
			"historical_summaries",            // 27
		)
	}
	if version >= int(clparams.ElectraVersion) {
		names = append(names,
			"deposit_requests_start_index",    // 28
			"deposit_balance_to_consume",      // 29
			"exit_balance_to_consume",         // 30
			"earliest_exit_epoch",             // 31
			"consolidation_balance_to_consume",// 32
			"earliest_consolidation_epoch",    // 33
			"pending_deposits",                // 34
			"pending_partial_withdrawals",     // 35
			"pending_consolidations",          // 36
		)
	}
	if version >= int(clparams.FuluVersion) {
		names = append(names, "proposer_lookahead") // 37
	}
	return names
}

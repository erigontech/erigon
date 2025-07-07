package smtv2

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/memdb"
	"github.com/stretchr/testify/assert"
)

type combinedTestInput struct {
	phase1Input []InputTapeItem
	phase1Root  common.Hash
	phase2Input []InputTapeItem
	phase2Root  common.Hash
	changeSet   []ChangeSetEntry
	debugTree   bool
	debugTapes  bool
}

func Test_CombinedStackAndTapes(t *testing.T) {
	tests := map[string]combinedTestInput{
		"empty db with simple add to the left": {
			phase1Input: []InputTapeItem{},
			phase1Root:  common.HexToHash("0x0"),
			phase2Input: []InputTapeItem{
				{
					Type:  IntersInputType_Value,
					Key:   UintToSmtKey(0),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
			},
			phase2Root: common.HexToHash("0x42bb2f66296df03552203ae337815976ca9c1bf52cc1bdd59399ede8fea8a822"),
			changeSet: []ChangeSetEntry{
				{
					Type:  ChangeSetEntryType_Add,
					Key:   UintToSmtKey(0),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
			},
			debugTapes: false,
			debugTree:  false,
		},
		"empty db with simple add to the right": {
			phase1Input: []InputTapeItem{},
			phase1Root:  common.HexToHash("0x0"),
			phase2Input: []InputTapeItem{
				{
					Type:  IntersInputType_Value,
					Key:   UintToSmtKey(1),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
			},
			phase2Root: common.HexToHash("0xb26e0de762d186d2efc35d9ff4388def6c96ec15f942d83d779141386fe1d2e1"),
			changeSet: []ChangeSetEntry{
				{
					Type:  ChangeSetEntryType_Add,
					Key:   UintToSmtKey(1),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
			},
		},
		"empty db with two keys and no depth": {
			phase1Input: []InputTapeItem{},
			phase1Root:  common.HexToHash("0x0"),
			phase2Input: []InputTapeItem{
				{
					Type:  IntersInputType_Value,
					Key:   UintToSmtKey(0),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
				{
					Type:  IntersInputType_Value,
					Key:   UintToSmtKey(1),
					Value: ScalarToSmtValue8FromBits(big.NewInt(2)),
				},
			},
			phase2Root: common.HexToHash("0x8500ebd22ea34240552b4656fdc8df9ca878879093b107177357c066c619173"),
			changeSet: []ChangeSetEntry{
				{
					Type:  ChangeSetEntryType_Add,
					Key:   UintToSmtKey(0),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
				{
					Type:  ChangeSetEntryType_Add,
					Key:   UintToSmtKey(1),
					Value: ScalarToSmtValue8FromBits(big.NewInt(2)),
				},
			},
		},
		"db with two keys and no depth, changng one value": {
			phase1Input: []InputTapeItem{
				{
					Type:  IntersInputType_Value,
					Key:   UintToSmtKey(0),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
				{
					Type:  IntersInputType_Value,
					Key:   UintToSmtKey(1),
					Value: ScalarToSmtValue8FromBits(big.NewInt(2)),
				},
			},
			phase1Root: common.HexToHash("0x8500ebd22ea34240552b4656fdc8df9ca878879093b107177357c066c619173"),
			phase2Input: []InputTapeItem{
				{
					Type:  IntersInputType_Value,
					Key:   UintToSmtKey(0),
					Value: ScalarToSmtValue8FromBits(big.NewInt(1)),
				},
				{
					Type:  IntersInputType_Value,
					Key:   UintToSmtKey(1),
					Value: ScalarToSmtValue8FromBits(big.NewInt(3)),
				},
			},
			phase2Root: common.HexToHash("0xc908c00e91ed46788fc03dd9a3fc6a0d15b609ec8c53d2cc284c1590292040b1"),
			changeSet: []ChangeSetEntry{
				{
					Type:          ChangeSetEntryType_Change,
					Key:           UintToSmtKey(1),
					Value:         ScalarToSmtValue8FromBits(big.NewInt(3)),
					OriginalValue: ScalarToSmtValue8FromBits(big.NewInt(2)),
				},
			},
		},
		"complex test 1": {
			phase1Input: makeInputTape(
				"0b0-0x01",
				"0b10-0x02",
				"0b110-0x03",
				"0b1110-0x04",
				"0b11110-0x05",
				"0b111110-0x06",
				"0b1111110-0x07",
			),
			phase1Root: common.HexToHash("0xdbb511e7e61746edfb84799c962bc412f1476e67af78bb62a845be66c481fd18"),
			phase2Input: makeInputTape(
				"0b1010-0x09",
				"0b110-0x03",
				"0b1110-0x04",
				"0b11110-0x15",
				"0b111110-0x06",
				"0b1111110-0x07",
				"0b1111111-0x08",
			),
			phase2Root: common.HexToHash("0xf51653dbbdf517e21adbe319a279e820c3f45c0c187aa2445ad2b19a2d94c190"),
			changeSet: []ChangeSetEntry{
				makeChangeSet(ChangeSetEntryType_Delete, "0b0", "0x01", "0x01"),
				makeChangeSet(ChangeSetEntryType_Delete, "0b10", "0x02", "0x02"),
				makeChangeSet(ChangeSetEntryType_Add, "0b1010", "0x09", "0x0"),
				makeChangeSet(ChangeSetEntryType_Change, "0b11110", "0x15", "0x05"),
				makeChangeSet(ChangeSetEntryType_Add, "0b1111111", "0x08", "0x0"),
			},
			debugTapes: false,
		},
		"complex test 2": {
			phase1Input: makeInputTape(
				"0b0011-0xAA",
				"0b0100-0xBB",
				"0b1101-0xCC",
				"0b11011-0xDD",
				"0b110111-0xEE",
				"0b1111-0xFF",
			),
			phase1Root: common.HexToHash("0xb5201f4a5ed17d2b7722a007b8ac20bd84918a205005b33cea4db8a8b69f08be"),
			phase2Input: makeInputTape(
				"0b0100-0xB0",
				"0b1010-0xA1",
				"0b1100-0xC0",
				"0b1101-0xCC",
				"0b110111-0xEE",
				"0b1111-0xF0",
			),
			phase2Root: common.HexToHash("0xaaf8a3bf98cbfa3e8b697c58d1f6a3d0fd702d8513b95981d03673c85a7e211d"),
			changeSet: []ChangeSetEntry{
				makeChangeSet(ChangeSetEntryType_Change, "0b0100", "0xB0", "0xBB"),
				makeChangeSet(ChangeSetEntryType_Add, "0b1010", "0xA1", "0x0"),
				makeChangeSet(ChangeSetEntryType_Delete, "0b0011", "0xAA", "0xAA"),
				makeChangeSet(ChangeSetEntryType_Add, "0b1100", "0xC0", "0x0"),
				makeChangeSet(ChangeSetEntryType_Change, "0b1111", "0xF0", "0xFF"),
				makeChangeSet(ChangeSetEntryType_Delete, "0b11011", "0xDD", "0xDD"),
			},
			debugTree:  false,
			debugTapes: false,
		},
		"balanced tree operations": {
			phase1Input: makeInputTape(
				"0b0000-0x11",
				"0b0011-0x22",
				"0b0101-0x33",
				"0b1001-0x55",
				"0b1011-0x66",
				"0b1101-0x77",
				"0b1111-0x88",
			),
			phase1Root: common.HexToHash("0x9427457cb6c470e547cd1e1a2bb48988245bf044bd15c654748199473ee504e9"),
			phase2Input: makeInputTape(
				"0b0000-0x11",
				"0b0011-0x29",
				"0b0110-0x34",
				"0b1001-0x55",
				"0b1011-0x66",
				"0b1101-0x79",
				"0b1111-0x88",
			),
			phase2Root: common.HexToHash("0x20bb8b01785108225190c64e76c478ed58197f6a463c58ba7ff515e0e30b9b91"),
			changeSet: []ChangeSetEntry{
				makeChangeSet(ChangeSetEntryType_Change, "0b0011", "0x29", "0x22"),
				makeChangeSet(ChangeSetEntryType_Delete, "0b0101", "0x33", "0x33"),
				makeChangeSet(ChangeSetEntryType_Add, "0b0110", "0x34", "0x0"),
				makeChangeSet(ChangeSetEntryType_Change, "0b1101", "0x79", "0x77"),
			},
			debugTree:  false,
			debugTapes: false,
		},
		"deep path operations": {
			phase1Input: makeInputTape(
				"0b000000-0xA1",
				"0b001100-0xB2",
				"0b010101-0xC3",
				"0b011111-0xD4",
				"0b101010-0xE5",
				"0b110011-0xF6",
			),
			phase1Root: common.HexToHash("0xbc75d2cf19fc656ebd0c94a1021a4e0ea3ba48563190087332255054140bba6"),
			phase2Input: makeInputTape(
				"0b000000-0xA9",
				"0b010101-0xC3",
				"0b011111-0xD4",
				"0b101010-0xE5",
				"0b110011-0xF6",
				"0b111111-0x1A",
			),
			phase2Root: common.HexToHash("0xa9c757fb2565ca8aab60000f24bf43e6a585ebb0c8e64493414119238141fa99"),
			changeSet: []ChangeSetEntry{
				makeChangeSet(ChangeSetEntryType_Change, "0b000000", "0xA9", "0xA1"),
				makeChangeSet(ChangeSetEntryType_Delete, "0b001100", "0xB2", "0xB2"),
				makeChangeSet(ChangeSetEntryType_Add, "0b111111", "0x1A", "0x0"),
			},
			debugTree:  false,
			debugTapes: false,
		},
		"sparse tree operations": {
			phase1Input: makeInputTape(
				"0b000001-0x1A",
				"0b001011-0x2B",
				"0b010111-0x3C",
				"0b100001-0x4D",
				"0b110101-0x5E",
			),
			phase1Root: common.HexToHash("0x1f85c456e3eeb9ded6b20605ab01aa9516759e71e7c31368808c94a4f88600fb"),
			phase2Input: makeInputTape(
				"0b000001-0x1F",
				"0b010111-0x3C",
				"0b011100-0x9A",
				"0b100001-0x4D",
				"0b101011-0x8B",
				"0b110101-0x5E",
			),
			phase2Root: common.HexToHash("0x4353b1df2ad7ee3ff5adceb4725dfcae8315ad15c1559d82d9f7a0ef518cb4c0"),
			changeSet: []ChangeSetEntry{
				makeChangeSet(ChangeSetEntryType_Change, "0b000001", "0x1F", "0x1A"),
				makeChangeSet(ChangeSetEntryType_Delete, "0b001011", "0x2B", "0x2B"),
				makeChangeSet(ChangeSetEntryType_Add, "0b011100", "0x9A", "0x0"),
				makeChangeSet(ChangeSetEntryType_Add, "0b101011", "0x8B", "0x0"),
			},
			debugTree:  false,
			debugTapes: false,
		},
		"simple sibling deletion on the left (rightmost sibling deleted)": {
			phase1Input: makeInputTape(
				"0b0000-0x1",
				"0b0001-0x2",
			),
			phase1Root: common.HexToHash("0xd72cd9ec2554e350b7cd038f37636a8d142c578f31726667854bf3eeb72a274"),
			phase2Input: makeInputTape(
				"0b0000-0x1",
			),
			phase2Root: common.HexToHash("0x42bb2f66296df03552203ae337815976ca9c1bf52cc1bdd59399ede8fea8a822"),
			changeSet: []ChangeSetEntry{
				makeChangeSet(ChangeSetEntryType_Delete, "0b0001", "0x2", "0x2"),
			},
			debugTree:  false,
			debugTapes: false,
		},
		"simple sibling deletion on the left (leftmost sibling deleted)": {
			phase1Input: makeInputTape(
				"0b0000-0x1",
				"0b0001-0x2",
			),
			phase1Root: common.HexToHash("0xd72cd9ec2554e350b7cd038f37636a8d142c578f31726667854bf3eeb72a274"),
			phase2Input: makeInputTape(
				"0b0001-0x2",
			),
			phase2Root: common.HexToHash("0xa41aa08f92fae23c84975caf086a472f20ad1eb0a0c56e3840f65fd620511a9"),
			changeSet: []ChangeSetEntry{
				makeChangeSet(ChangeSetEntryType_Delete, "0b0000", "0x1", "0x1"),
			},
			debugTree:  false,
			debugTapes: false,
		},
		"simple sibling deletion on the right (rightmost sibling deleted)": {
			phase1Input: makeInputTape(
				"0b1110-0x1",
				"0b1111-0x2",
			),
			phase1Root: common.HexToHash("0xf3fc1800ec03eee979ebab09103a99539d2f902dd988dc3bbd7e7baa2fa99914"),
			phase2Input: makeInputTape(
				"0b1110-0x1",
			),
			phase2Root: common.HexToHash("0x689fdf5fa6a0a82153a4bd711ac7b9ed76c395285a46a77f3328074df35dcdec"),
			changeSet: []ChangeSetEntry{
				makeChangeSet(ChangeSetEntryType_Delete, "0b1111", "0x2", "0x2"),
			},
			debugTree:  false,
			debugTapes: false,
		},
		"simple sibling deletion on the right (leftmost sibling deleted)": {
			phase1Input: makeInputTape(
				"0b1110-0x1",
				"0b1111-0x2",
			),
			phase1Root: common.HexToHash("0xf3fc1800ec03eee979ebab09103a99539d2f902dd988dc3bbd7e7baa2fa99914"),
			phase2Input: makeInputTape(
				"0b1111-0x2",
			),
			phase2Root: common.HexToHash("0x6ec96a608c18e55fed575c131055cef740a4821fbaf5c4fcb8d4e6e4870022b0"),
			changeSet: []ChangeSetEntry{
				makeChangeSet(ChangeSetEntryType_Delete, "0b1110", "0x1", "0x1"),
			},
			debugTree:  false,
			debugTapes: false,
		},
		"two siblings deleted does not leave branch IH artefacts": {
			phase1Input: makeInputTape(
				"0b10000-0x4",
			),
			phase1Root: common.HexToHash("0x4dce1a9ff1266e00e8463d8f71b12f31f03d872a2610b44b9935b6771f3e661a"),
			phase2Input: makeInputTape(
				"0b00100-0x1",
				"0b00000-0x2",
				"0b00001-0x3",
				"0b10000-0x4",
			),
			phase2Root: common.HexToHash("0x52b7ae55ee0f050283786f57f59a901e8382c1051a6cd74c4c9a4b125b0b3b90"),
			changeSet: []ChangeSetEntry{
				makeChangeSet(ChangeSetEntryType_Add, "0b00100", "0x1", "0x1"),
				makeChangeSet(ChangeSetEntryType_Add, "0b00000", "0x2", "0x2"),
				makeChangeSet(ChangeSetEntryType_Add, "0b00001", "0x3", "0x3"),
			},
			debugTree:  false,
			debugTapes: false,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			//
			// prep instances of everyting ready for phase 1 and phase 2 where we get the roots and collect interhashes
			// for the initial and final states.  We do this before we run the changeset in.
			//
			phase1UntouchedDb := memdb.NewTestDB(t)
			defer phase1UntouchedDb.Close()
			phase1UntouchedTx, err := phase1UntouchedDb.BeginRw(context.Background())
			assert.NoError(t, err)
			defer phase1UntouchedTx.Rollback()
			phase1UntouchedTx.CreateBucket(kv.TableSmtIntermediateHashes)

			phase1Db := memdb.NewTestDB(t)
			defer phase1Db.Close()
			phase1Tx, err := phase1Db.BeginRw(context.Background())
			assert.NoError(t, err)
			defer phase1Tx.Rollback()
			phase1Tx.CreateBucket(kv.TableSmtIntermediateHashes)

			phase2Db := memdb.NewTestDB(t)
			defer phase2Db.Close()
			phase2Tx, err := phase2Db.BeginRw(context.Background())
			assert.NoError(t, err)
			defer phase2Tx.Rollback()
			phase2Tx.CreateBucket(kv.TableSmtIntermediateHashes)

			// first run phase 1 and phase 2 inputs to get the roots for the start and end and keep hold of the
			// interhashes from the collectors
			untouchedIterator := &MockInputTapeIterator{}
			untouchedIterator.items = lexographicalSortInputTape(test.phase1Input)
			phase1Iterator := &MockInputTapeIterator{}
			phase1Iterator.items = lexographicalSortInputTape(test.phase1Input)
			phase2Iterator := &MockInputTapeIterator{}
			phase2Iterator.items = lexographicalSortInputTape(test.phase2Input)

			untouchedCollector, err := NewEtlIntermediateHashCollector(os.TempDir())
			assert.NoError(t, err)
			defer untouchedCollector.Close()
			phase1Collector, err := NewEtlIntermediateHashCollector(os.TempDir())
			assert.NoError(t, err)
			defer phase1Collector.Close()
			phase2Collector, err := NewEtlIntermediateHashCollector(os.TempDir())
			assert.NoError(t, err)
			defer phase2Collector.Close()
			phase3Collector, err := NewEtlIntermediateHashCollector(os.TempDir())
			assert.NoError(t, err)
			defer phase3Collector.Close()
			reverseCollector, err := NewEtlIntermediateHashCollector(os.TempDir())
			assert.NoError(t, err)
			defer reverseCollector.Close()

			untouchedHasher := NewSmtStackHasher(untouchedIterator, untouchedCollector)
			untouchedHasher.SetDebug(test.debugTree)
			phase1Hasher := NewSmtStackHasher(phase1Iterator, phase1Collector)
			phase1Hasher.SetDebug(test.debugTree)
			phase2Hasher := NewSmtStackHasher(phase2Iterator, phase2Collector)
			phase2Hasher.SetDebug(test.debugTree)

			//
			// get phase 1 untouched root
			//
			untouchedRoot, untouchedFinalNode, _, err := untouchedHasher.RegenerateRoot("untouched", 10*time.Second)
			assert.NoError(t, err)
			untouchedRootHash := common.BigToHash(untouchedRoot)
			if assert.Equal(t, test.phase1Root, untouchedRootHash) {
				fmt.Println("--------------------------------")
				fmt.Printf("Phase 1 Untouched Root matches: %s\n", untouchedRootHash.Hex())
				fmt.Println("--------------------------------")
			}
			err = untouchedCollector.Load("phase-1-untouched", 10*time.Second, phase1UntouchedTx, kv.TableSmtIntermediateHashes)
			assert.NoError(t, err)
			err = HandleFinalHash(phase1UntouchedTx, untouchedFinalNode)
			assert.NoError(t, err)

			//
			// get phase 1 root
			//
			phase1Root, phase1FinalNode, _, err := phase1Hasher.RegenerateRoot("phase-1", 10*time.Second)
			assert.NoError(t, err)
			phase1RootHash := common.BigToHash(phase1Root)
			if assert.Equal(t, test.phase1Root, phase1RootHash) {
				fmt.Println("--------------------------------")
				fmt.Printf("Phase 1 Root matches: %s\n", phase1RootHash.Hex())
				fmt.Println("--------------------------------")
			}
			err = phase1Collector.Load("phase-1", 10*time.Second, phase1Tx, kv.TableSmtIntermediateHashes)
			assert.NoError(t, err)
			err = HandleFinalHash(phase1Tx, phase1FinalNode)
			assert.NoError(t, err)

			//
			// get phase 2 root
			//
			phase2Root, phase2FinalNode, _, err := phase2Hasher.RegenerateRoot("phase-2", 10*time.Second)
			assert.NoError(t, err)
			phase2RootHash := common.BigToHash(phase2Root)
			if assert.Equal(t, test.phase2Root, phase2RootHash) {
				fmt.Println("--------------------------------")
				fmt.Printf("Phase 2 Root matches: %s\n", phase2RootHash.Hex())
				fmt.Println("--------------------------------")
			}
			err = phase2Collector.Load("phase-2", 10*time.Second, phase2Tx, kv.TableSmtIntermediateHashes)
			assert.NoError(t, err)
			err = HandleFinalHash(phase2Tx, phase2FinalNode)
			assert.NoError(t, err)

			//
			// prep and run the change set in from phase 1
			//
			changeSetTape := NewChangeSetTape()
			sortedChangeSet := LexoSortChangeSet(test.changeSet[:])
			for _, entry := range sortedChangeSet {
				changeSetTape.Add(entry.Type, entry.Key, entry.Value, entry.OriginalValue)
			}
			changeSetTape.Add(ChangeSetEntryType_Terminator, nodeKeyFromPath([]int{}), ScalarToSmtValue8FromBits(big.NewInt(0)), ScalarToSmtValue8FromBits(big.NewInt(0)))

			interhashTape, err := NewDbInterhashTape(phase1Tx)
			assert.NoError(t, err)
			defer interhashTape.Close()

			tapesProcessor, err := NewTapesProcessor(changeSetTape, interhashTape, test.debugTapes)
			assert.NoError(t, err)

			phase3Hashes := NewSmtStackHasher(tapesProcessor, phase3Collector)
			phase3Hashes.SetDebug(test.debugTree)

			if test.debugTapes {
				func() {
					fmt.Printf("--------------------------------\n")
					fmt.Printf("Phase 3 Inputs\n")
					fmt.Printf("Change Set Tape:---------\n")
					for _, item := range changeSetTape.entries {
						fmt.Printf("%s\n", item.String(10))
					}
					fmt.Printf("Interhash Tape:---------\n")
					c, err := phase1Tx.Cursor(kv.TableSmtIntermediateHashes)
					assert.NoError(t, err)
					defer c.Close()
					for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
						assert.NoError(t, err)
						ih := DeserialiseIntermediateHash(v)
						fmt.Printf("%s\n", ih.String())
					}
					fmt.Printf("Output Tape:---------\n")
					for _, item := range tapesProcessor.outputTape.history {
						fmt.Printf("%s\n", item.String(10))
					}
					fmt.Printf("--------------------------------\n")
				}()
			}

			phase3Root, phase3FinalNode, _, err := phase3Hashes.RegenerateRoot("phase-3", 10*time.Second)
			assert.NoError(t, err)
			phase3RootHash := common.BigToHash(phase3Root)
			if assert.Equal(t, phase2RootHash, phase3RootHash) {
				fmt.Println("--------------------------------")
				fmt.Printf("Phase 3 Root matches: %s\n", phase3RootHash.Hex())
				fmt.Println("--------------------------------")
			}
			// load the phase 3 changes into the initial state so that we can compare it to the output of phase 2 state
			// they should be identical
			err = phase3Collector.Load("phase-3", 10*time.Second, phase1Tx, kv.TableSmtIntermediateHashes)
			assert.NoError(t, err)
			err = HandleFinalHash(phase1Tx, phase3FinalNode)
			assert.NoError(t, err)

			//
			// now compare the db contents of intial + changeset vs phase 2
			//
			c1, err := phase1Tx.Cursor(kv.TableSmtIntermediateHashes)
			assert.NoError(t, err)
			defer c1.Close()
			c2, err := phase2Tx.Cursor(kv.TableSmtIntermediateHashes)
			assert.NoError(t, err)
			defer c2.Close()

			count1, err := c1.Count()
			assert.NoError(t, err)
			count2, err := c2.Count()
			assert.NoError(t, err)
			assert.Equal(t, count2, count1, "the number of interhashes in the db should be the same")

			// iterate over the two cursors ensuring the contents are identical
			for k1, v1, err := c1.Next(); k1 != nil; k1, v1, err = c1.Next() {
				assert.NoError(t, err)
				k2, v2, err := c2.Next()
				assert.NoError(t, err)
				assert.Equal(t, k1, k2)
				assert.Equal(t, v1, v2)
			}

			// now we need to reverse the changesets in the test to unwind the changes to the SMT and ensure we get the original
			// root from phase 1
			reversedChangeSet := InvertChangeSet(sortedChangeSet[:])
			reversedChangeSetTape := NewChangeSetTape()
			for _, entry := range reversedChangeSet {
				reversedChangeSetTape.Add(entry.Type, entry.Key, entry.Value, entry.OriginalValue)
			}
			reversedChangeSetTape.Add(ChangeSetEntryType_Terminator, nodeKeyFromPath([]int{}), ScalarToSmtValue8FromBits(big.NewInt(0)), ScalarToSmtValue8FromBits(big.NewInt(0)))

			reversedInterhashTape, err := NewDbInterhashTape(phase1Tx)
			assert.NoError(t, err)
			defer reversedInterhashTape.Close()

			reversedTapesProcessor, err := NewTapesProcessor(reversedChangeSetTape, reversedInterhashTape, test.debugTapes)
			assert.NoError(t, err)

			reversePhaseHasher := NewSmtStackHasher(reversedTapesProcessor, reverseCollector)
			reversePhaseHasher.SetDebug(test.debugTree)

			if test.debugTapes {
				func() {
					fmt.Printf("--------------------------------\n")
					fmt.Printf("Reverse Phase Inputs\n")
					fmt.Printf("Change Set Tape:---------\n")
					for _, item := range reversedChangeSetTape.entries {
						fmt.Printf("%s\n", item.String(10))
					}
					fmt.Printf("Interhash Tape:---------\n")
					c, err := phase1Tx.Cursor(kv.TableSmtIntermediateHashes)
					assert.NoError(t, err)
					defer c.Close()
					for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
						assert.NoError(t, err)
						ih := DeserialiseIntermediateHash(v)
						fmt.Printf("%s\n", ih.String())
					}
					fmt.Printf("Output Tape:---------\n")
					for _, item := range reversedTapesProcessor.outputTape.history {
						fmt.Printf("%s\n", item.String(10))
					}
					fmt.Printf("--------------------------------\n")
				}()
			}

			reversedPhaseRoot, reversedPhaseFinalNode, _, err := reversePhaseHasher.RegenerateRoot("reversed", 10*time.Second)
			assert.NoError(t, err)
			reversedPhaseRootHash := common.BigToHash(reversedPhaseRoot)
			if assert.Equal(t, phase1RootHash, reversedPhaseRootHash) {
				fmt.Println("--------------------------------")
				fmt.Printf("Reverse Phase Root matches: %s\n", reversedPhaseRootHash.Hex())
				fmt.Println("--------------------------------")
			}
			err = reverseCollector.Load("phase-3", 10*time.Second, phase1Tx, kv.TableSmtIntermediateHashes)
			assert.NoError(t, err)
			err = HandleFinalHash(phase1Tx, reversedPhaseFinalNode)
			assert.NoError(t, err)

			// now finally compare the untouched db contents of phase 1 with the reversed changeset contents
			// they should be identical
			untouchedCursor, err := phase1UntouchedTx.Cursor(kv.TableSmtIntermediateHashes)
			assert.NoError(t, err)
			defer untouchedCursor.Close()
			reversedCursor, err := phase1Tx.Cursor(kv.TableSmtIntermediateHashes)
			assert.NoError(t, err)
			defer reversedCursor.Close()

			untouchedCount, err := untouchedCursor.Count()
			assert.NoError(t, err)
			reversedCount, err := reversedCursor.Count()
			assert.NoError(t, err)
			assert.Equal(t, untouchedCount, reversedCount, "the number of interhashes in the db should be the same")

			somethingWrong := false
			expectedKeys := [][]byte{}
			actualKeys := [][]byte{}
			expectedValues := [][]byte{}
			actualValues := [][]byte{}
			for k1, v1, err := untouchedCursor.Next(); k1 != nil; k1, v1, err = untouchedCursor.Next() {
				assert.NoError(t, err)
				k2, v2, err := reversedCursor.Next()
				assert.NoError(t, err)
				expectedKeys = append(expectedKeys, k1)
				actualKeys = append(actualKeys, k2)
				expectedValues = append(expectedValues, v1)
				actualValues = append(actualValues, v2)
				if !bytes.Equal(k1, k2) {
					somethingWrong = true
				}
				if !bytes.Equal(v1, v2) {
					somethingWrong = true
				}
			}

			if somethingWrong {
				fmt.Printf("expected db:\n")
				for i, key := range expectedKeys {
					key := key[:key[256]]
					fmt.Printf("%v: %v\n", key, expectedValues[i])
					fmt.Printf("--------------------------------\n")
				}
				fmt.Printf("actual db:\n")
				for i, key := range actualKeys {
					key := key[:key[256]]
					fmt.Printf("%v: %v\n", key, actualValues[i])
					fmt.Printf("--------------------------------\n")
				}
			}

			assert.False(t, somethingWrong, "the contents of the db should be the same")
		})
	}
}

func binaryStringToNodeKey(binaryString string) []int {
	binaryString = strings.TrimPrefix(binaryString, "0b")
	path := []int{}
	for _, char := range binaryString {
		if char == '1' {
			path = append(path, 1)
		} else {
			path = append(path, 0)
		}
	}

	return path
}

func hexToSmtValue8(hex string) SmtValue8 {
	hex = strings.TrimPrefix(hex, "0x")
	value, ok := new(big.Int).SetString(hex, 16)
	if !ok {
		panic(fmt.Sprintf("invalid hex value %s", hex))
	}
	return ScalarToSmtValue8FromBits(value)
}

func makeChangeSet(typ ChangeSetEntryType, binaryPath, hexValue, originalValue string) ChangeSetEntry {
	return ChangeSetEntry{
		Type:          typ,
		Key:           nodeKeyFromPath(binaryStringToNodeKey(binaryPath)),
		Value:         hexToSmtValue8(hexValue),
		OriginalValue: hexToSmtValue8(originalValue),
	}
}

func lexographicalSortInputTape(inputTape []InputTapeItem) []InputTapeItem {
	slices.SortFunc(inputTape, func(a, b InputTapeItem) int {
		return lexographhicalCheckPaths(a.Key.GetPath(), b.Key.GetPath())
	})
	return inputTape
}

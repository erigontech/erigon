package smt_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/ledgerwatch/erigon/smt/pkg/db"
	"github.com/ledgerwatch/erigon/smt/pkg/smt"
	"github.com/ledgerwatch/erigon/smt/pkg/utils"
	"gotest.tools/v3/assert"
)

func assertSmtDbStructure(t *testing.T, s *smt.SMT, testMetadata bool) {
	smtBatchRootHash, _ := s.Db.GetLastRoot()

	actualDb, ok := s.Db.(*db.MemDb)
	if !ok {
		return
	}

	usedNodeHashesMap := make(map[string]*utils.NodeKey)
	assertSmtTreeDbStructure(t, s, utils.ScalarToRoot(smtBatchRootHash), usedNodeHashesMap)

	// EXPLAIN THE LINE BELOW: db could have more values because values' hashes are not deleted
	assert.Equal(t, true, len(actualDb.Db)-len(usedNodeHashesMap) >= 0)
	for k := range usedNodeHashesMap {
		_, found := actualDb.Db[k]
		assert.Equal(t, true, found)
	}

	totalLeaves := assertHashToKeyDbStrcture(t, s, utils.ScalarToRoot(smtBatchRootHash), testMetadata)
	assert.Equal(t, totalLeaves, len(actualDb.DbHashKey))
	if testMetadata {
		assert.Equal(t, totalLeaves, len(actualDb.DbKeySource))
	}

	assertTraverse(t, s)
}

func assertSmtTreeDbStructure(t *testing.T, s *smt.SMT, nodeHash utils.NodeKey, usedNodeHashesMap map[string]*utils.NodeKey) {
	if nodeHash.IsZero() {
		return
	}

	dbNodeValue, err := s.Db.Get(nodeHash)
	assert.NilError(t, err)

	nodeHashHex := utils.ConvertBigIntToHex(utils.ArrayToScalar(nodeHash[:]))
	usedNodeHashesMap[nodeHashHex] = &nodeHash

	if dbNodeValue.IsFinalNode() {
		nodeValueHash := utils.NodeKeyFromBigIntArray(dbNodeValue[4:8])
		dbNodeValue, err = s.Db.Get(nodeValueHash)
		assert.NilError(t, err)

		nodeHashHex := utils.ConvertBigIntToHex(utils.ArrayToScalar(nodeValueHash[:]))
		usedNodeHashesMap[nodeHashHex] = &nodeValueHash
		return
	}

	assertSmtTreeDbStructure(t, s, utils.NodeKeyFromBigIntArray(dbNodeValue[0:4]), usedNodeHashesMap)
	assertSmtTreeDbStructure(t, s, utils.NodeKeyFromBigIntArray(dbNodeValue[4:8]), usedNodeHashesMap)
}

func assertHashToKeyDbStrcture(t *testing.T, smtBatch *smt.SMT, nodeHash utils.NodeKey, testMetadata bool) int {
	if nodeHash.IsZero() {
		return 0
	}

	dbNodeValue, err := smtBatch.Db.Get(nodeHash)
	assert.NilError(t, err)

	if dbNodeValue.IsFinalNode() {
		memDb := smtBatch.Db.(*db.MemDb)

		nodeKey, err := smtBatch.Db.GetHashKey(nodeHash)
		assert.NilError(t, err)

		keyConc := utils.ArrayToScalar(nodeHash[:])
		k := utils.ConvertBigIntToHex(keyConc)
		_, found := memDb.DbHashKey[k]
		assert.Equal(t, found, true)

		if testMetadata {
			keyConc = utils.ArrayToScalar(nodeKey[:])

			_, found = memDb.DbKeySource[keyConc.String()]
			assert.Equal(t, found, true)
		}
		return 1
	}

	return assertHashToKeyDbStrcture(t, smtBatch, utils.NodeKeyFromBigIntArray(dbNodeValue[0:4]), testMetadata) + assertHashToKeyDbStrcture(t, smtBatch, utils.NodeKeyFromBigIntArray(dbNodeValue[4:8]), testMetadata)
}

func assertTraverse(t *testing.T, s *smt.SMT) {
	smtBatchRootHash, _ := s.Db.GetLastRoot()

	ctx := context.Background()
	action := func(prefix []byte, k utils.NodeKey, v utils.NodeValue12) (bool, error) {
		if v.IsFinalNode() {
			valHash := v.Get4to8()
			v, err := s.Db.Get(*valHash)
			if err != nil {
				return false, err
			}

			if v[0] == nil {
				return false, fmt.Errorf("value is missing in the db")
			}

			vInBytes := utils.ArrayBigToScalar(utils.BigIntArrayFromNodeValue8(v.GetNodeValue8())).Bytes()
			if vInBytes == nil {
				return false, fmt.Errorf("error in converting to bytes")
			}

			return false, nil
		}

		return true, nil
	}
	err := s.Traverse(ctx, smtBatchRootHash, action)
	assert.NilError(t, err)
}

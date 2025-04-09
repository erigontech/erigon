package das

import (
	"encoding/binary"
	"fmt"
	"math/big"
	"sort"

	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/p2p/enode"
)

type DataColumnSidecar struct {
	Index uint64 `json:"index"`
	// Column *solid.ListSSZ[Cell] `json:"column"`
	KzgCommitments               *solid.ListSSZ[*cltypes.KZGCommitment] `json:"kzg_commitments"`
	KzgProofs                    *solid.ListSSZ[*cltypes.KZGProof]      `json:"kzg_proofs"`
	SignedBlockHeader            *cltypes.SignedBeaconBlockHeader       `json:"signed_block_header"`
	KzgCommitmentsInclusionProof solid.HashVectorSSZ                    `json:"kzg_commitments_inclusion_proof"`
}

type MatrixEntry struct {
	Cell        Cell              `json:"cell"`
	KzgProof    *cltypes.KZGProof `json:"kzg_proof"`
	ColumnIndex ColumnIndex       `json:"column_index"`
	RowIndex    RowIndex          `json:"row_index"`
}

type Cell []byte

type CustodyIndex uint64
type ColumnIndex uint64
type RowIndex uint64

// func GetCustodyGroups(nodeId
func GetCustodyGroups(nodeId enode.ID, custodyGroupCount uint64) ([]CustodyIndex, error) {
	if custodyGroupCount > numberOfCustodyGroups {
		return nil, fmt.Errorf("custodyGroupCount is greater than the number of custody groups")
	}
	currentId, ok := new(big.Int).SetString(nodeId.String(), 16)
	if !ok {
		return nil, fmt.Errorf("failed to convert nodeId to big int")
	}
	maxUint256 := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	custodyGroups := make([]CustodyIndex, 0)
	for uint64(len(custodyGroups)) < custodyGroupCount {
		// Hash current ID and take first 8 bytes
		hash := crypto.Keccak256(currentId.Bytes())
		custodyGroup := binary.LittleEndian.Uint64(hash[:8]) % numberOfCustodyGroups

		// Check if custody group already exists
		exists := false
		for _, g := range custodyGroups {
			if g == CustodyIndex(custodyGroup) {
				exists = true
				break
			}
		}

		if !exists {
			custodyGroups = append(custodyGroups, CustodyIndex(custodyGroup))
		}

		// Increment currentId with overflow protection
		if currentId.Cmp(maxUint256) == 0 {
			currentId.SetInt64(0)
		} else {
			currentId.Add(currentId, big.NewInt(1))
		}
	}

	// Sort custody groups
	sort.Slice(custodyGroups, func(i, j int) bool {
		return custodyGroups[i] < custodyGroups[j]
	})

	return custodyGroups, nil
}

// GetColumnsForCustodyGroup returns the column indices that belong to a given custody group.
func GetColumnsForCustodyGroup(custodyGroup CustodyIndex) ([]ColumnIndex, error) {
	if custodyGroup >= numberOfCustodyGroups {
		return nil, fmt.Errorf("custody group %d is greater than or equal to the number of custody groups (%d)", custodyGroup, numberOfCustodyGroups)
	}

	columnsPerGroup := numberOfColumns / numberOfCustodyGroups
	columns := make([]ColumnIndex, columnsPerGroup)

	for i := ColumnIndex(0); i < ColumnIndex(columnsPerGroup); i++ {
		columns[i] = ColumnIndex(numberOfCustodyGroups*uint64(i) + uint64(custodyGroup))
	}

	return columns, nil
}

// ComputeMatrix takes a slice of blobs and returns a flattened sequence of matrix entries.
func ComputeMatrix(blobs [][]byte) ([]MatrixEntry, error) {
	matrix := make([]MatrixEntry, 0, len(blobs)*int(numberOfColumns))

	for blobIndex, blob := range blobs {
		cells, proofs, err := ComputeCellsAndKZGProofs(blob) // in kzg pkg
		if err != nil {
			return nil, fmt.Errorf("failed to compute cells and proofs for blob %d: %w", blobIndex, err)
		}

		for cellIndex := range cells {
			matrix = append(matrix, MatrixEntry{
				Cell:        cells[cellIndex],
				KzgProof:    proofs[cellIndex],
				RowIndex:    RowIndex(blobIndex),
				ColumnIndex: ColumnIndex(cellIndex),
			})
		}
	}

	return matrix, nil
}

// RecoverMatrix takes a partial matrix and the total blob count and returns a complete matrix.
func RecoverMatrix(partialMatrix []MatrixEntry, blobCount uint64) ([]MatrixEntry, error) {
	matrix := make([]MatrixEntry, 0, blobCount*numberOfColumns)

	// Process each blob row
	for blobIndex := uint64(0); blobIndex < blobCount; blobIndex++ {
		// Get cells and column indices for this blob row
		var cellIndices []ColumnIndex
		var cells []Cell
		for _, entry := range partialMatrix {
			if entry.RowIndex == RowIndex(blobIndex) {
				cellIndices = append(cellIndices, entry.ColumnIndex)
				cells = append(cells, entry.Cell)
			}
		}

		// Recover the full row of cells and proofs
		recoveredCells, recoveredProofs, err := RecoverCellsAndKZGProofs(cellIndices, cells) // in kzg pkg
		if err != nil {
			return nil, fmt.Errorf("failed to recover cells and proofs for blob %d: %w", blobIndex, err)
		}

		// Add recovered entries to matrix
		for cellIndex := range recoveredCells {
			matrix = append(matrix, MatrixEntry{
				Cell:        recoveredCells[cellIndex],
				KzgProof:    recoveredProofs[cellIndex],
				RowIndex:    RowIndex(blobIndex),
				ColumnIndex: ColumnIndex(cellIndex),
			})
		}
	}

	return matrix, nil
}

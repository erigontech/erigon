package eth

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/holiman/uint256"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/chain"
)

func TestStartExecutionBeforeCliqueTransition(t *testing.T) {
	config := &chain.Config{
		ChainID:                 uint256.NewInt(59141),
		Clique:                  &chain.CliqueConfig{Period: 1, Epoch: 30000},
		TerminalTotalDifficulty: uint256.NewInt(37331807),
	}
	require.True(t, shouldStartExecution(config, func() *uint256.Int { return uint256.NewInt(1) }))
	require.True(t, shouldStartExecution(config, func() *uint256.Int { return nil }))
	config.Clique = nil
	require.False(t, shouldStartExecution(config, func() *uint256.Int { return uint256.NewInt(1) }))
	require.True(t, shouldStartExecution(config, func() *uint256.Int { return uint256.NewInt(37331807) }))
}

func TestRemoveContents(t *testing.T) {
	tmpDirName := t.TempDir()
	//t.Logf("creating %s/root...", rootName)
	rootName := filepath.Join(tmpDirName, "root")
	err := os.Mkdir(rootName, 0750)
	require.NoError(t, err)
	//fmt.Println("OK")
	for i := range 3 {
		outerName := filepath.Join(rootName, fmt.Sprintf("outer_%d", i+1))
		//t.Logf("creating %s... ", outerName)
		err = os.Mkdir(outerName, 0750)
		require.NoError(t, err)
		//t.Logf("OK")
		for j := range 2 {
			innerName := filepath.Join(outerName, fmt.Sprintf("inner_%d", j+1))
			//t.Logf("creating %s... ", innerName)
			err = os.Mkdir(innerName, 0750)
			require.NoError(t, err)
			//t.Log("OK")
			for k := range 2 {
				innestName := filepath.Join(innerName, fmt.Sprintf("innest_%d", k+1))
				//t.Logf("creating %s... ", innestName)
				err = os.Mkdir(innestName, 0750)
				require.NoError(t, err)
				//t.Log("OK")
			}
		}
	}
	list, err := os.ReadDir(rootName)
	require.NoError(t, err)

	require.Len(t, list, 3)

	err = RemoveContents(rootName)
	require.NoError(t, err)

	list, err = os.ReadDir(rootName)
	require.NoError(t, err)

	require.Empty(t, list)
}

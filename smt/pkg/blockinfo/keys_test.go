package blockinfo

import (
	"math/big"
	"testing"

	"github.com/ledgerwatch/erigon/smt/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func TestKeyBlockHeaderParams(t *testing.T) {
	scenarios := map[string]struct {
		param      *big.Int
		expected   utils.NodeKey
		shouldFail bool
	}{
		"KeyBlockHash": {
			param:      big.NewInt(IndexBlockHeaderParamBlockHash),
			expected:   utils.NodeKey{17540094328570681229, 15492539097581145461, 7686481670809850401, 16577991319572125169},
			shouldFail: false,
		},
		"KeyCoinbase": {
			param:      big.NewInt(IndexBlockHeaderParamCoinbase),
			expected:   utils.NodeKey{13866806033333411216, 11510953292839890698, 8274877395843603978, 9372332419316597113},
			shouldFail: false,
		},
		"KeyBlockNumber": {
			param:      big.NewInt(IndexBlockHeaderParamNumber),
			expected:   utils.NodeKey{6024064788222257862, 13049342112699253445, 12127984136733687200, 8398043461199794462},
			shouldFail: false,
		},
		"KeyGasLimit": {
			param:      big.NewInt(IndexBlockHeaderParamGasLimit),
			expected:   utils.NodeKey{5319681466197319121, 14057433120745733551, 5638531288094714593, 17204828339478940337},
			shouldFail: false,
		},
		"KeyTimestamp": {
			param:      big.NewInt(IndexBlockHeaderParamTimestamp),
			expected:   utils.NodeKey{7890158832167317866, 11032486557242372179, 9653801891436451408, 2062577087515942703},
			shouldFail: false,
		},
		"KeyGer": {
			param:      big.NewInt(IndexBlockHeaderParamGer),
			expected:   utils.NodeKey{16031278424721309229, 4132999715765882778, 6388713709192801251, 10826219431775251904},
			shouldFail: false,
		},
		"KeyBlockHashL1": {
			param:      big.NewInt(IndexBlockHeaderParamBlockHashL1),
			expected:   utils.NodeKey{5354929451503733866, 3129555839551084896, 2132809659008379950, 8230742270813566472},
			shouldFail: false,
		},
		"NilKey": {
			param:      nil,
			expected:   utils.NodeKey{},
			shouldFail: true,
		},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			val, err := KeyBlockHeaderParams(scenario.param)
			if scenario.shouldFail {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, scenario.expected, *val)
			}
		})
	}
}

func TestKeyTxLogs(t *testing.T) {
	scenarios := map[string]struct {
		txIndex    *big.Int
		logIndex   *big.Int
		expected   utils.NodeKey
		shouldFail bool
	}{
		"Zeros": {
			txIndex:    big.NewInt(0),
			logIndex:   big.NewInt(0),
			expected:   utils.NodeKey{11419009644269063127, 648930505133020521, 13599491301354210104, 15823077770165791231},
			shouldFail: false,
		},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			val, err := KeyTxLogs(scenario.txIndex, scenario.logIndex)
			if scenario.shouldFail {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, scenario.expected, *val)
			}
		})
	}
}

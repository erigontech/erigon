package smt

import (
	"fmt"

	"github.com/ledgerwatch/erigon/smt/pkg/utils"
)

var (
	ErrEmptySearchPath = fmt.Errorf("search path is empty")
)

func (s *SMT) GetNodeAtPath(path []int) (nodeV *utils.NodeValue12, err error) {
	pathLen := len(path)
	if pathLen == 0 {
		return nil, ErrEmptySearchPath
	}

	var sl utils.NodeValue12

	oldRoot, err := s.getLastRoot()
	if err != nil {
		return nil, fmt.Errorf("getLastRoot: %w", err)
	}

	for level, pathByte := range path {
		sl, err = s.Db.Get(oldRoot)
		if err != nil {
			return nil, err
		}

		if sl.IsFinalNode() {
			foundRKey := utils.NodeKeyFromBigIntArray(sl[0:4])
			if level < pathLen-1 ||
				foundRKey.GetPath()[0] != pathByte {
				return nil, nil
			}

			break
		} else {
			oldRoot = utils.NodeKeyFromBigIntArray(sl[pathByte*4 : pathByte*4+4])
			if oldRoot.IsZero() {
				return nil, nil
			}
		}
	}

	return &sl, nil
}

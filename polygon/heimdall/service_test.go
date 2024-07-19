// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package heimdall

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/polygon/bor"
	"github.com/erigontech/erigon/polygon/bor/valset"
	"github.com/erigontech/erigon/polygon/polygoncommon"
	"github.com/erigontech/erigon/turbo/testlog"
)

func TestConfirmCantHave2RwTxAtSameTime(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	logger := testlog.Logger(t, log.LvlDebug)
	dataDir := t.TempDir()
	db := polygoncommon.NewDatabase(dataDir, logger)
	err := db.OpenOnce(ctx, kv.HeimdallDB, databaseTablesCfg)
	require.NoError(t, err)

	tx1, err := db.BeginRw(ctx)
	require.NoError(t, err)
	t.Cleanup(tx1.Rollback)

	tx2, err := db.BeginRw(ctx)
	require.NoError(t, err)
	t.Cleanup(tx2.Rollback)

	//
	// TODO need to introduce some mutex at inside the MdbxEntityStore layer
	//
}

func TestSpanProducerSelection(t *testing.T) {
	// do for span 0
	spanZero := &Span{}
	validatorSet := valset.NewValidatorSet(spanZero.Producers())
	accumPriorities := SpanAccumProducerPriorities{
		SpanId:    SpanIdAt(0),
		Producers: validatorSet.Validators,
	}

	// then onwards for every new span need to do
	// 1. validatorSet.IncrementProposerPriority(sprintCountInSpan)
	// 2. validatorSet.UpdateWithChangeSet
	logger := testlog.Logger(t, log.LvlDebug)
	newSpan := &Span{}
	validatorSet = bor.GetUpdatedValidatorSet(validatorSet, newSpan.Producers(), logger)
	validatorSet.IncrementProposerPriority(1)
	accumPriorities = SpanAccumProducerPriorities{
		SpanId:    SpanId(1),
		Producers: validatorSet.Validators,
	}

	// have a span producers tracker component that the heimdall service uses
	// it registers for receiving new span updates
	// upon changing from span X to span Y it performs UpdateWithChangeSet
	// and persists the new producer priorities in the DB
	// TODO implement this component
}

package commands

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	kv2 "github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/rawdbhelpers"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/polygon/heimdall"

	log "github.com/erigontech/erigon/common/log/v3"
)

type StageProgress struct {
	Stage    stages.SyncStage
	Progress uint64
	PrunedTo uint64
}

type Last struct {
	TxNum    uint64
	BlockNum uint64
	IdxSteps float64
}

type Snapshot struct {
	SegMax uint64
	IndMax uint64
}

type DB struct {
	FirstHeader uint64
	LastHeader  uint64
	FirstBody   uint64
	LastBody    uint64
}

type StagesInfo struct {
	StagesProgress   []StageProgress
	PruneDistance    prune.Mode
	SnapshotInfo     Snapshot
	BorSnapshotInfo  Snapshot
	LastInfo         Last
	EthTxSequence    uint64
	DB               DB
	DomainIIProgress []DomainIIProgress
	ChainInfo        ChainInfo
}

type ChainInfo struct {
	ChainID   uint64
	ChainName string
}

func (info *StagesInfo) OverviewTUI() string {
	return fmt.Sprintf(
		"[yellow]Chain:[-] ID [cyan]%d[-]  Name [green]%s[-]\n"+
			"[yellow]Prune mode:[-] %s\n"+
			"[yellow]Blocks:[-] seg: [cyan]%d[-]  ind: [cyan]%d[-]\n"+
			"[yellow]Bor blocks:[-] seg: [cyan]%d[-]  ind: [cyan]%d[-]\n"+
			"[yellow]Last & state.history:[-] txnum: [cyan]%d[-], blocknum: [cyan]%d[-], steps: [magenta]%.2f[-]\n"+
			"[yellow]EthTxSequence:[-] [cyan]%d[-]\n"+
			"[yellow]In DB:[-] first header [cyan]%d[-], last header [cyan]%d[-],\n"+
			"first body [cyan]%d[-], last body [cyan]%d[-]\n"+
			"[yellow]Last update on[-] [::d]%s[-]",
		info.ChainInfo.ChainID, info.ChainInfo.ChainName,
		info.PruneDistance.String(),
		info.SnapshotInfo.SegMax, info.SnapshotInfo.IndMax,
		info.BorSnapshotInfo.SegMax, info.BorSnapshotInfo.IndMax,
		info.LastInfo.TxNum, info.LastInfo.BlockNum, info.LastInfo.IdxSteps,
		info.EthTxSequence,
		info.DB.FirstHeader, info.DB.LastHeader, info.DB.FirstBody, info.DB.LastBody,
		time.Now().Format("15:04:05"))
}

func (info *StagesInfo) Stages() string {
	res := "Stages:\n" + fmt.Sprintf("%-15s %12s %12s\n", "stage_at", "progress", "prune_at")
	res += strings.Repeat("-", 43) + "\n"
	for _, s := range info.StagesProgress {
		res += fmt.Sprintf("%-15s %12d %12d\n", s.Stage, s.Progress, s.PrunedTo)
	}
	return res
}

func (info *StagesInfo) DomainII() string {
	var b strings.Builder

	fmt.Fprintf(&b, "%-12s %18s %18s %18s\n",
		"domain or ii name", "historyStartFrom", "progress(txnum)", "progress(step)")
	b.WriteString(strings.Repeat("-", 70) + "\n")

	for _, d := range info.DomainIIProgress {
		history := "-"
		if d.HistoryStartFrom != 0 {
			history = fmt.Sprintf("%d", d.HistoryStartFrom)
		}
		txnum := "-"
		if d.TxNum != 0 {
			txnum = fmt.Sprintf("%d", d.TxNum)
		}
		step := "-"
		if d.Step != 0 {
			step = fmt.Sprintf("%d", d.Step)
		}

		fmt.Fprintf(&b, "%-12s %18s %18s %18s\n",
			d.Name, history, txnum, step)
	}

	return b.String()
}

type DomainIIProgress struct {
	HistoryStartFrom uint64
	Name             string
	TxNum            uint64
	Step             uint64
}

// InfoStages collects stage progress and domain/index info from a temporal transaction.
func InfoStages(tx kv.TemporalTx, snapshots *freezeblocks.RoSnapshots, borSn *heimdall.RoSnapshots) (info *StagesInfo, err error) {
	defer func() {
		recovered := recover()
		if recovered != nil {
			err = fmt.Errorf("recovered from panic: %v", recovered)
		}
	}()
	var progress uint64
	info = &StagesInfo{}

	// Read chain config without ChainConfigWithErr (which doesn't exist in main)
	genesisHash, hashErr := rawdb.ReadCanonicalHash(tx, 0)
	if hashErr == nil {
		cfg, cfgErr := rawdb.ReadChainConfig(tx, genesisHash)
		if cfgErr == nil && cfg != nil {
			info.ChainInfo = ChainInfo{
				ChainID:   cfg.ChainID.Uint64(),
				ChainName: cfg.ChainName,
			}
		}
	}

	info.StagesProgress = make([]StageProgress, 0, len(stages.AllStages))
	for _, stage := range stages.AllStages {
		if progress, err = stages.GetStageProgress(tx, stage); err != nil {
			return nil, err
		}
		prunedTo, err := stages.GetStagePruneProgress(tx, stage)
		if err != nil {
			return nil, err
		}
		info.StagesProgress = append(info.StagesProgress, StageProgress{
			Stage:    stage,
			PrunedTo: prunedTo,
			Progress: progress,
		})
	}
	pm, err := prune.Get(tx)
	if err != nil {
		return nil, err
	}
	info.PruneDistance = pm
	if snapshots != nil {
		info.SnapshotInfo = Snapshot{
			SegMax: snapshots.SegmentsMax(),
			IndMax: snapshots.IndicesMax(),
		}
	}
	if borSn != nil {
		info.BorSnapshotInfo = Snapshot{
			SegMax: borSn.SegmentsMax(),
			IndMax: borSn.IndicesMax(),
		}
	}

	_lb, _lt, _ := rawdbv3.TxNums.Last(tx)

	dbg := tx.Debug()
	stepSize := dbg.StepSize()

	info.LastInfo = Last{
		TxNum:    _lt,
		BlockNum: _lb,
		IdxSteps: rawdbhelpers.IdxStepsCountV3(tx, stepSize),
	}
	ethTxSequence, err := tx.ReadSequence(kv.EthTx)
	if err != nil {
		return nil, err
	}
	info.EthTxSequence = ethTxSequence

	{
		firstNonGenesisHeader, err := rawdbv3.SecondKey(tx, kv.Headers)
		if err != nil {
			return nil, err
		}
		lastHeaders, err := rawdbv3.LastKey(tx, kv.Headers)
		if err != nil {
			return nil, err
		}
		firstNonGenesisBody, err := rawdbv3.SecondKey(tx, kv.BlockBody)
		if err != nil {
			return nil, err
		}
		lastBody, err := rawdbv3.LastKey(tx, kv.BlockBody)
		if err != nil {
			return nil, err
		}
		fstHeader := infoU64or0(firstNonGenesisHeader)
		lstHeader := infoU64or0(lastHeaders)
		fstBody := infoU64or0(firstNonGenesisBody)
		lstBody := infoU64or0(lastBody)
		info.DB = DB{
			FirstHeader: fstHeader,
			LastHeader:  lstHeader,
			FirstBody:   fstBody,
			LastBody:    lstBody,
		}
	}

	info.DomainIIProgress = make([]DomainIIProgress, 0, kv.DomainLen+4)
	for i := 0; i < int(kv.DomainLen); i++ {
		d := kv.Domain(i)
		txNum := dbg.DomainProgress(d)
		step := txNum / stepSize
		if d == kv.CommitmentDomain {
			info.DomainIIProgress = append(info.DomainIIProgress, DomainIIProgress{
				Name: d.String(),
				Step: step,
			})
			continue
		}
		info.DomainIIProgress = append(info.DomainIIProgress, DomainIIProgress{
			Name:             d.String(),
			HistoryStartFrom: dbg.HistoryStartFrom(d),
			TxNum:            txNum,
			Step:             step,
		})
	}
	for _, ii := range []kv.InvertedIdx{kv.LogTopicIdx, kv.LogAddrIdx, kv.TracesFromIdx, kv.TracesToIdx} {
		txNum := dbg.IIProgress(ii)
		step := txNum / stepSize
		info.DomainIIProgress = append(info.DomainIIProgress, DomainIIProgress{
			Name:  ii.String(),
			TxNum: txNum,
			Step:  step,
		})
	}

	return info, nil
}

// infoU64or0 decodes a big-endian uint64 from a byte slice, returning 0 for nil/empty input.
// Named differently from u64or0 in reset_state.go to avoid redeclaration.
func infoU64or0(in []byte) (v uint64) {
	if len(in) > 0 {
		v = binary.BigEndian.Uint64(in)
	}
	return v
}

// InfoAllStages opens the datadir DB read-only and streams StagesInfo to infoCh.
// It is the entry point called by the etui TUI.
func InfoAllStages(ctx context.Context, logger log.Logger, dataDirPath string, infoCh chan<- *StagesInfo) error {
	dirs := datadir.New(dataDirPath)
	chainDataPath := dirs.Chaindata

	opts := kv2.New(dbcfg.ChainDB, logger).
		Path(chainDataPath).
		Accede(true) // open read-only, don't create

	db, err := openDB(opts, false, "", logger)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer db.Close()

	sn, borSn, _, _, _, _, _ := allSnapshots(ctx, db, logger) // ignore error to get partial stat
	if sn != nil {
		defer sn.Close()
	}
	if borSn != nil {
		defer borSn.Close()
	}

	// Single read then close
	return db.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		info, err := InfoStages(tx, sn, borSn)
		if err != nil {
			return err
		}
		select {
		case infoCh <- info:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	})
}

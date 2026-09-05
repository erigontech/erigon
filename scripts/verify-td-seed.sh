#!/usr/bin/env bash
# verify-td-seed.sh — focused check that snapshot-derived TD seeding
# leaves no holes.
#
# TD lives only in MDBX; snapshots do not carry it. FillDBFromSnapshots
# seeds it once at postIndexed, and ExtendTDFromSnapshots fills whatever
# arrives afterwards. A hole in kv.HeaderTD wedges any unwind whose
# target lands in it ("parent's total difficulty not found"), and there
# is deliberately no repair at unwind time — the seed is expected to be
# correct.
#
# This exercises only bootstrap, not the unwind matrix: a full soak
# spends hours to answer a question one bootstrap settles. Cycles 25-28
# each burned a whole run to report the same gap.
#
# PASS: kv.HeaderTD has zero gaps and the seed watermark is consistent
# with what was written.
#
# Env:
#   SETTLE_SEC  seconds to keep running after the postIndexed seed, so
#               later-arriving files exercise the incremental pass
#               (default 900).
#   KEEP_DATADIR=1  don't wipe the datadir on exit.

set -u

cd /erigon/mark/hive/clients/erigon/erigon

RESULTS_DIR=${RESULTS_DIR:-/erigon/tmp/td-verify}
mkdir -p "$RESULTS_DIR"
run=$(printf '%03d' "$(( $(ls "$RESULTS_DIR" 2>/dev/null | grep -c '^run-') + 1 ))")
out="$RESULTS_DIR/run-$run"
mkdir -p "$out"

DATADIR=${DATADIR:-/erigon/tmp/erigon-td-verify.run-$run}
LOG="$out/erigon.log"
SETTLE_SEC=${SETTLE_SEC:-900}

echo "[td-verify] run $run → $out  datadir=$DATADIR  settle=${SETTLE_SEC}s"

pkill -f "datadir=$DATADIR" 2>/dev/null
sleep 2
rm -rf "$DATADIR"; mkdir -p "$DATADIR"

DATADIR="$DATADIR" LOG="$LOG" nohup scripts/erigon-launch-hoodi-soak.sh </dev/null >/dev/null 2>&1 &
sleep 5
ELPID=$(pgrep -f "datadir=$DATADIR" | head -1)
echo "[td-verify] launched pid=$ELPID"

cleanup() {
    [[ -n "${ELPID:-}" ]] && kill "$ELPID" 2>/dev/null
    for _ in $(seq 1 30); do kill -0 "$ELPID" 2>/dev/null || break; sleep 1; done
    kill -9 "$ELPID" 2>/dev/null
    sleep 3
}

# Wait for the one-shot seed, then keep running so files that arrive
# afterwards go through the incremental path — the case every previous
# cycle actually failed.
echo "[td-verify] waiting for postIndexed seed (cap 45m)"
seeded=0
for i in $(seq 1 540); do
    if grep -q "postIndexed: OpenFolder" "$LOG" 2>/dev/null; then seeded=1; break; fi
    kill -0 "$ELPID" 2>/dev/null || { echo "[td-verify] erigon exited early"; break; }
    sleep 5
done
if [[ "$seeded" -ne 1 ]]; then
    echo "[td-verify] FAIL: postIndexed seed never ran"
    cleanup; exit 1
fi
echo "[td-verify] seed done at $(date -u +%T); settling ${SETTLE_SEC}s"
sleep "$SETTLE_SEC"

cleanup

probe=$(mktemp -d)
cat > "$probe/main.go" <<'GO'
package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
)

func main() {
	db, err := mdbx.New(dbcfg.ChainDB, log.New()).Path(os.Args[1]+"/chaindata").Accede(true).Readonly(true).Open(context.Background())
	if err != nil {
		fmt.Println("open err:", err)
		os.Exit(2)
	}
	defer db.Close()
	gaps := 0
	_ = db.View(context.Background(), func(tx kv.Tx) error {
		seed, _ := stages.GetStageProgress(tx, stages.SyncStage("SnapshotsSeed"))
		fmt.Printf("SnapshotsSeed watermark = %d\n", seed)
		c, _ := tx.Cursor(kv.HeaderTD)
		defer c.Close()
		k, _, _ := c.First()
		var prev, first, last uint64
		firstRow := true
		for k != nil {
			bn := binary.BigEndian.Uint64(k[:8])
			if firstRow {
				first, firstRow = bn, false
			} else if bn > prev+1 {
				if gaps < 5 {
					fmt.Printf("  TD gap (%d, %d) missing %d\n", prev, bn, bn-prev-1)
				}
				gaps++
			}
			prev, last = bn, bn
			k, _, _ = c.Next()
		}
		fmt.Printf("HeaderTD span=[%d,%d] gaps=%d\n", first, last, gaps)
		if seed > last {
			fmt.Printf("  watermark %d exceeds highest TD row %d\n", seed, last)
			gaps++
		}
		return nil
	})
	os.Exit(map[bool]int{true: 0, false: 1}[gaps == 0])
}
GO
cp go.mod go.sum "$probe/" 2>/dev/null
echo "[td-verify] probing $DATADIR"
go run "$probe/main.go" "$DATADIR" 2>&1 | grep -vE "^#|note:|warning" | tee "$out/probe.txt"
rc=${PIPESTATUS[0]}
rm -rf "$probe"

grep -E "header coverage break" "$LOG" 2>/dev/null | tail -3 | tee -a "$out/probe.txt"

[[ "${KEEP_DATADIR:-0}" == "1" ]] || rm -rf "$DATADIR"

if [[ "$rc" -eq 0 ]]; then
    echo "[td-verify] PASS: run $run — no TD gaps"
    exit 0
fi
echo "[td-verify] FAIL: run $run — TD gaps present (see $out/probe.txt)"
exit 1

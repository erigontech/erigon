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

//go:build gorules
// +build gorules

package gorules

// to apply changes in this file, please do: ./build/bin/golangci-lint cache clean
import (
	"github.com/quasilyte/go-ruleguard/dsl"
	//quasilyterules "github.com/quasilyte/ruleguard-rules-test"
)

func init() {
	//dsl.ImportRules("qrules", quasilyterules.Bundle)
}

func txDeferRollback(m dsl.Matcher) {
	// Common pattern for long-living transactions:
	//	tx, err := db.Begin()
	//	if err != nil {
	//		return err
	//	}
	//	defer tx.Rollback()
	//
	//	... code which uses database in transaction
	//
	//	err := tx.Commit()
	//	if err != nil {
	//		return err
	//	}

	m.Match(
		`$tx, $err := $db.BeginRw($ctx); $chk; $rollback`,
		`$tx, $err = $db.BeginRw($ctx); $chk; $rollback`,
		`$tx, $err := $db.Begin($ctx); $chk; $rollback`,
		`$tx, $err = $db.Begin($ctx); $chk; $rollback`,
		`$tx, $err := $db.BeginRo($ctx); $chk; $rollback`,
		`$tx, $err = $db.BeginRo($ctx); $chk; $rollback`,
		`$tx, $err = $db.BeginTemporalRw($ctx); $chk; $rollback`,
		`$tx, $err := $db.BeginTemporalRw($ctx); $chk; $rollback`,
		`$tx, $err = $db.BeginTemporalRo($ctx); $chk; $rollback`,
		`$tx, $err := $db.BeginTemporalRo($ctx); $chk; $rollback`,
		`$tx, $err := $db.BeginRwNosync($ctx); $chk; $rollback`,
		`$tx, $err = $db.BeginRwNosync($ctx); $chk; $rollback`,
	).
		Where(!m["rollback"].Text.Matches(`defer .*\.Rollback()`)).
		//At(m["rollback"]).
		Report(`Add "defer $tx.Rollback()" right after transaction creation error check. 
			If you are in the loop - consider use "$db.View" or "$db.Update" or extract whole transaction to function.
			Without rollback in defer - app can deadlock on error or panic.
			Rules are in ./rules.go file.
			`)
}

func cursorDeferClose(m dsl.Matcher) {
	m.Match(
		`$c, $err = $db.Cursor($table); $chk; $close`,
		`$c, $err := $db.Cursor($table); $chk; $close`,
		`$c, $err = $db.RwCursor($table); $chk; $close`,
		`$c, $err := $db.RwCursor($table); $chk; $close`,
		`$c, $err = $db.CursorDupSort($table); $chk; $close`,
		`$c, $err := $db.CursorDupSort($table); $chk; $close`,
		`$c, $err = $db.RwCursorDupSort($table); $chk; $close`,
		`$c, $err := $db.RwCursorDupSort($table); $chk; $close`,
	).
		Where(!m["close"].Text.Matches(`defer .*\.Close()`)).
		//At(m["rollback"]).
		Report(`Add "defer $c.Close()" right after cursor creation error check`)
}

func streamDeferClose(m dsl.Matcher) {
	m.Match(
		`$c, $err = $db.Range($params); $chk; $close`,
		`$c, $err := $db.Range($params); $chk; $close`,
		`$c, $err = $db.RangeDupSort($params); $chk; $close`,
		`$c, $err := $db.RangeDupSort($params); $chk; $close`,
		`$c, $err = $db.Prefix($params); $chk; $close`,
		`$c, $err := $db.Prefix($params); $chk; $close`,
	).
		Where(!m["close"].Text.Matches(`defer .*\.Close()`)).
		//At(m["rollback"]).
		Report(`Add "defer $c.Close()" right after cursor creation error check`)
}

func closeCollector(m dsl.Matcher) {
	m.Match(`$c := etl.NewCollector($*_); $close`).
		Where(!m["close"].Text.Matches(`defer .*\.Close()`)).
		Report(`Add "defer $c.Close()" right after collector creation`)
	m.Match(`$c := etl.NewCollectorWithAllocator($*_); $close`).
		Where(!m["close"].Text.Matches(`defer .*\.Close()`)).
		Report(`Add "defer $c.Close()" right after collector creation`)
}

func closeLockedDir(m dsl.Matcher) {
	m.Match(`$c := dir.OpenRw($*_); $close`).
		Where(!m["close"].Text.Matches(`defer .*\.Close()`)).
		Report(`Add "defer $c.Close()" after locked.OpenDir`)
}

func passValuesByContext(m dsl.Matcher) {
	m.Match(`ctx.WithValue($*_)`).Report(`Don't pass app-level parameters by context, pass them as-is or as typed objects`)
}

func mismatchingUnlock(m dsl.Matcher) {
	// By default, an entire match position is used as a location.
	// This can be changed by the At() method that binds the location
	// to the provided named submatch.
	//
	// In the rules below text editor would get mismatching method
	// name locations:
	//
	//   defer mu.RUnlock()
	//            ^^^^^^^

	m.Match(`$mu.Lock(); defer $mu.$unlock()`).
		Where(m["unlock"].Text == "RUnlock").
		At(m["unlock"]).
		Report(`maybe $mu.Unlock() was intended?
			Rules are in ./rules.go file.`)

	m.Match(`$mu.RLock(); defer $mu.$unlock()`).
		Where(m["unlock"].Text == "Unlock").
		At(m["unlock"]).
		Report(`maybe $mu.RUnlock() was intended?
			Rules are in ./rules.go file.`)
}

func filepathWalkToCheckToSkipNonExistingFiles(m dsl.Matcher) {
	m.Match(`filepath.Walk($dir, $cb)`).Report(`report("Use filepath.WalkDir or fs.WalkDir, because Walk does not skip removed files and does much more syscalls")`)
}

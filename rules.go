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

package gorules

// to apply changes in this file, please do: ./build/bin/golangci-lint cache clean
import (
	"github.com/quasilyte/go-ruleguard/dsl"
)

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
		`$tx, $err := $db.$begin($ctx); $chk; $rollback`,
		`$tx, $err = $db.$begin($ctx); $chk; $rollback`,
	).
		Where((m["begin"].Text == "Begin" ||
			m["begin"].Text == "BeginRw" ||
			m["begin"].Text == "BeginRo" ||
			m["begin"].Text == "BeginRwNosync" ||
			m["begin"].Text == "BeginTemporalRw" ||
			m["begin"].Text == "BeginTemporalRo") &&
			!m["rollback"].Text.Matches(`\.Rollback\(\)`) &&
			!m["rollback"].Text.Matches(`t\.Cleanup\(.*\.Rollback\)`)).
		//At(m["rollback"]).
		Report(`Add "defer $tx.Rollback()" or "t.Cleanup($tx.Rollback)" right after transaction creation error check. 
			If you are in the loop - consider using "$db.View" or "$db.Update" or extract whole transaction to function.
			Without rollback in defer - app can deadlock on error or panic.
			Rules are in ./rules.go file.
			`)
}

func cursorDeferClose(m dsl.Matcher) {
	m.Match(
		`$c, $err := $db.$cur($table); $chk; $close`,
		`$c, $err = $db.$cur($table); $chk; $close`,
	).
		Where((m["cur"].Text == "Cursor" ||
			m["cur"].Text == "RwCursor" ||
			m["cur"].Text == "CursorDupSort" ||
			m["cur"].Text == "RwCursorDupSort") &&
			!m["close"].Text.Matches(`\.Close\(\)`) &&
			!m["close"].Text.Matches(`t\.Cleanup\(.*\.Close\)`)).
		//At(m["close"]).
		Report(`Add "defer $c.Close()" or "t.Cleanup($c.Close)" right after cursor creation error check`)
}

func streamDeferClose(m dsl.Matcher) {
	m.Match(
		`$c, $err := $db.$stream($params); $chk; $close`,
		`$c, $err = $db.$stream($params); $chk; $close`,
	).
		Where((m["stream"].Text == "Range" ||
			m["stream"].Text == "RangeDupSort" ||
			m["stream"].Text == "Prefix") &&
			!m["close"].Text.Matches(`\.Close\(\)`) &&
			!m["close"].Text.Matches(`t\.Cleanup\(.*\.Close\)`)).
		//At(m["close"]).
		Report(`Add "defer $c.Close()" or "t.Cleanup($c.Close)" right after cursor creation error check`)
}

func closeCollector(m dsl.Matcher) {
	m.Match(`$c := etl.$newCol($*_); $close`).
		Where((m["newCol"].Text == "NewCollector" ||
			m["newCol"].Text == "NewCollectorWithAllocator") &&
			!m["close"].Text.Matches(`\.Close\(\)`)).
		Report(`Add "defer $c.Close()" right after collector creation`)
}

func closeLockedDir(m dsl.Matcher) {
	m.Match(`$c := dir.OpenRw($*_); $close`).
		Where(!m["close"].Text.Matches(`\.Close\(\)`)).
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
		Report(`Did you mean $mu.Unlock()?
			Rules are in ./rules.go file.`)

	m.Match(`$mu.RLock(); defer $mu.$unlock()`).
		Where(m["unlock"].Text == "Unlock").
		At(m["unlock"]).
		Report(`Did you mean $mu.RUnlock()?
			Rules are in ./rules.go file.`)
}

func forbidOsRemove(m dsl.Matcher) {
	m.Match(`os.$rm($*_)`).
		Where(m["rm"].Text == "Remove" || m["rm"].Text == "RemoveAll").
		Report(`Don't call os.Remove/RemoveAll directly; use dir.RemoveFile/RemoveAll instead (erigon/common/dir)`)
}

func filepathWalkToCheckToSkipNonExistingFiles(m dsl.Matcher) {
	m.Match(`filepath.Walk($dir, $cb)`).Report(`report("Use filepath.WalkDir or fs.WalkDir, because Walk does not skip removed files and does much more syscalls")`)
}

func osCreateBlankAssign(m dsl.Matcher) {
	m.Match(
		`_, $_ := os.$fn($*_)`,
		`_, $_ = os.$fn($*_)`,
	).
		Where(m["fn"].Text == "Create" || m["fn"].Text == "CreateTemp" || m["fn"].Text == "OpenFile").
		Report(`os.Create/OpenFile result assigned to _ leaks a file descriptor. Assign to a variable and close it.
			Rules are in ./rules.go file.`)
}

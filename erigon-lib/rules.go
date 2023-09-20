//go:build gorules
// +build gorules

package gorules

// https://github.com/golang/go/wiki/Modules#how-can-i-track-tool-dependencies-for-a-module
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
	).
		Where(!m["rollback"].Text.Matches(`defer .*\.Rollback()`)).
		//At(m["rollback"]).
		Report(`Add "defer $tx.Rollback()" right after transaction creation error check. 
			If you are in the loop - consider use "$db.View" or "$db.Update" or extract whole transaction to function.
			Without rollback in defer - app can deadlock on error or panic.
			Rules are in ./rules.go file.
			`)
}

func closeCollector(m dsl.Matcher) {
	m.Match(`$c := etl.NewCollector($*_); $close`).
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
		Report(`maybe $2mu.Unlock() was intended?
			Rules are in ./rules.go file.`)

	m.Match(`$mu.RLock(); defer $mu.$unlock()`).
		Where(m["unlock"].Text == "Unlock").
		At(m["unlock"]).
		Report(`maybe $mu.RUnlock() was intended?
			Rules are in ./rules.go file.`)
}

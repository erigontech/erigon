package snapshotsync

import (
	"reflect"
	"testing"
)

func TestSnapshotMode(t *testing.T) {
	sm := SnapshotMode{}
	sm.Receipts = true
	if sm.ToString() != "r" {
		t.Fatal(sm.ToString())
	}
	sm.State = true
	if sm.ToString() != "sr" {
		t.Fatal(sm.ToString())
	}
	sm.Bodies = true
	if sm.ToString() != "bsr" {
		t.Fatal(sm.ToString())
	}
	sm.Headers = true
	if sm.ToString() != "hbsr" {
		t.Fatal(sm.ToString())
	}
}

func TestSnapshotModeFromString(t *testing.T) {
	sm, err := SnapshotModeFromString("hsbr")
	if err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(sm, SnapshotMode{
		Headers:  true,
		Bodies:   true,
		State:    true,
		Receipts: true,
	}) == false {
		t.Fatal(sm)
	}
}

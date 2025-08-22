package sysutils_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/diagnostics/sysutils"
)

func TestMergeProcesses(t *testing.T) {
	initaldata := [][]*sysutils.ProcessInfo{
		{
			{Pid: 1, Name: "test1", CPUUsage: 1.0, Memory: 1.0},
			{Pid: 2, Name: "test2", CPUUsage: 2.0, Memory: 2.0},
			{Pid: 3, Name: "test3", CPUUsage: 3.0, Memory: 3.0},
			{Pid: 31, Name: "test31", CPUUsage: 3.0, Memory: 3.0},
		},
		{
			{Pid: 1, Name: "test1", CPUUsage: 1.0, Memory: 1.0},
			{Pid: 2, Name: "test2", CPUUsage: 1.0, Memory: 1.0},
			{Pid: 22, Name: "test4", CPUUsage: 1.0, Memory: 1.0},
		},
	}

	expected := []*sysutils.ProcessInfo{
		{Pid: 1, Name: "test1", CPUUsage: 1.0, Memory: 1.0},
		{Pid: 2, Name: "test2", CPUUsage: 1.5, Memory: 1.5},
		{Pid: 3, Name: "test3", CPUUsage: 3.0, Memory: 3.0},
		{Pid: 31, Name: "test31", CPUUsage: 3.0, Memory: 3.0},
		{Pid: 22, Name: "test4", CPUUsage: 1.0, Memory: 1.0},
	}

	result := sysutils.MergeProcesses(initaldata)
	for _, proc := range result {
		require.Contains(t, expected, proc)
	}
}

func TestRemoveProcessesBelowThreshold(t *testing.T) {
	initaldata := [][]*sysutils.ProcessInfo{
		{
			{Pid: 1, Name: "test1", CPUUsage: 1.0, Memory: 1.0},
			{Pid: 2, Name: "test2", CPUUsage: 2.0, Memory: 2.0},
			{Pid: 3, Name: "test3", CPUUsage: 3.0, Memory: 3.0},
			{Pid: 12, Name: "test5", CPUUsage: 0.001, Memory: 1.0},
			{Pid: 45, Name: "test8", CPUUsage: 0.001, Memory: 0.0},
		},
		{
			{Pid: 1, Name: "test1", CPUUsage: 1.0, Memory: 1.0},
			{Pid: 2, Name: "test2", CPUUsage: 1.0, Memory: 1.0},
			{Pid: 22, Name: "test4", CPUUsage: 1.0, Memory: 0.001},
		},
	}

	expected := []*sysutils.ProcessInfo{
		{Pid: 1, Name: "test1", CPUUsage: 1.0, Memory: 1.0},
		{Pid: 2, Name: "test2", CPUUsage: 1.5, Memory: 1.5},
		{Pid: 3, Name: "test3", CPUUsage: 3.0, Memory: 3.0},
		{Pid: 22, Name: "test4", CPUUsage: 1.0, Memory: 0.001},
		{Pid: 12, Name: "test5", CPUUsage: 0.001, Memory: 1.0},
	}

	result := sysutils.MergeProcesses(initaldata)
	result = sysutils.RemoveProcessesBelowThreshold(result, 0.01)
	for _, proc := range result {
		require.Contains(t, expected, proc)
	}
}
